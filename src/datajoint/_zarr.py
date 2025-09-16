from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath, PureWindowsPath

import numpy.typing as npt
import zarr
from tqdm import tqdm
from typing_extensions import Final

from datajoint import errors, s3
from datajoint.declare import EXTERNAL_TABLE_ROOT
from datajoint.errors import DataJointError, MissingExternalFile
from datajoint.hash import uuid_from_buffer, uuid_from_file
from datajoint.heading import Heading
from datajoint.settings import config
from datajoint.table import FreeTable, Table
from datajoint.utils import safe_copy, safe_write

logger = logging.getLogger(__name__.split(".")[0])

CACHE_SUBFOLDING: Final = (
    2,
    2,
)  # (2, 2) means  "0123456789abcd" will be saved as "01/23/0123456789abcd"
SUPPORT_MIGRATED_BLOBS: Final = True  # support blobs migrated from datajoint 0.11.*

def get_uuid(data: zarr.Group | zarr.Array) -> uuid.UUID:
    """
    Get a UUID based on a Zarr hierarchy by hashing the store contents.
    """
    import hashlib

    # Create a hash based on the store contents
    hasher = hashlib.md5()
    
    # Get keys from store - different stores have different methods
    try:
        if hasattr(data.store, 'keys'):
            keys = list(data.store.keys())
        elif hasattr(data.store, 'listdir'):
            keys = data.store.listdir()
        else:
            # For MemoryStore and similar, we need to iterate differently
            keys = list(data.store)
    except Exception:
        # Fallback: use a simple hash based on the data itself
        if isinstance(data, zarr.Array):
            hasher.update(data[:].tobytes())
            hasher.update(str(data.shape).encode('utf-8'))
            hasher.update(str(data.dtype).encode('utf-8'))
        else:
            # For groups, hash the group name and array names
            hasher.update(data.name.encode('utf-8') if data.name else b'root')
            try:
                for key in data.keys():
                    hasher.update(key.encode('utf-8'))
                    if isinstance(data[key], zarr.Array):
                        hasher.update(data[key][:].tobytes())
            except:
                # If we can't iterate, just use a timestamp-based fallback
                import time
                hasher.update(str(time.time()).encode('utf-8'))
        hash_bytes = hasher.digest()
        return uuid.UUID(bytes=hash_bytes)
    
    # Sort keys to ensure consistent ordering
    sorted_keys = sorted(keys)
    
    for key in sorted_keys:
        # Add the key itself
        hasher.update(key.encode('utf-8'))
        
        # Add the data for this key
        try:
            value = data.store[key]
            if isinstance(value, bytes):
                hasher.update(value)
            else:
                hasher.update(str(value).encode('utf-8'))
        except Exception:
            # If we can't read the value, just hash the key
            pass
    
    # Convert MD5 hash to UUID format
    hash_bytes = hasher.digest()
    return uuid.UUID(bytes=hash_bytes)

def subfold(name: str, folds: tuple[int, ...]) -> tuple[str, ...]:
    """
    subfolding for external storage: e.g.  subfold('aBCdefg', (2, 3))  -->  ['ab','cde']
    """
    return (
        (name[: folds[0]].lower(),) + subfold(name[folds[0] :], folds[1:])
        if folds
        else ()
    )


class ExternalZarrTable(Table):
    """
    The table tracking externally stored objects, with special support for Zarr hierarchies.
    Declare as ExternalZarrTable(connection, database)
    """

    def __init__(self, connection, store, database):
        self.store = store
        self.spec = config.get_store_spec(store)
        self._s3 = None
        self.database = database
        self._connection = connection
        self._heading = Heading(
            table_info=dict(
                conn=connection,
                database=database,
                table_name=self.table_name,
                context=None,
            )
        )
        self._support = [self.full_table_name]
        if not self.is_declared:
            self.declare()
        self._s3 = None
        if self.spec["protocol"] == "file" and not Path(self.spec["location"]).is_dir():
            raise FileNotFoundError(
                "Inaccessible local directory %s" % self.spec["location"]
            ) from None

    @property
    def definition(self):
        return """
        # zarr storage tracking
        hash  : uuid    # hash of zarr store contents
        ---
        size      :bigint unsigned     # size of zarr store in bytes
        zarr_path=null : varchar(1000)  # path to zarr store in external storage
        timestamp=CURRENT_TIMESTAMP  :timestamp   # automatic timestamp
        """

    @property
    def table_name(self):
        return f"{EXTERNAL_TABLE_ROOT}_zarr_{self.store}"

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = s3.Folder(**self.spec)
        return self._s3

    # - low-level operations - private

    def _make_external_filepath(self, relative_filepath):
        """resolve the complete external path based on the relative path"""
        # Strip root
        if self.spec["protocol"] == "s3":
            posix_path = PurePosixPath(PureWindowsPath(self.spec["location"]))
            location_path = (
                Path(*posix_path.parts[1:])
                if len(self.spec["location"]) > 0
                and any(case in posix_path.parts[0] for case in ("\\", ":"))
                else Path(posix_path)
            )
            return PurePosixPath(location_path, relative_filepath)
        # Preserve root
        elif self.spec["protocol"] == "file":
            return PurePosixPath(Path(self.spec["location"]), relative_filepath)
        else:
            assert False

    def _make_uuid_path(self, uuid, suffix=""):
        """create external path based on the uuid hash"""
        return self._make_external_filepath(
            PurePosixPath(
                self.database,
                "/".join(subfold(uuid.hex, self.spec["subfolding"])),
                uuid.hex,
            ).with_suffix(suffix)
        )

    def _copy_store(self, source_store: zarr.abc.Store, dest_path: str, metadata=None):
        """
        Copy a Zarr store to external storage.
        """
        if self.spec["protocol"] == "s3":
            # For S3, use FSSpecStore
            import fsspec
            from zarr.storage import FSSpecStore

            fs = fsspec.filesystem('s3')
            dest_store = FSSpecStore(fs=fs, path=f"{self.spec['bucket']}/{dest_path}")

        elif self.spec["protocol"] == "file":
            # For file system, create the directory structure and use dict-like copying
            dest_path = Path(dest_path)
            dest_path.mkdir(parents=True, exist_ok=True)

            # For local file system, we can copy directly without LocalStore complications
            # Use a simple dict-like approach for now
            dest_store = {}
        else:
            raise ValueError(f"Unsupported protocol: {self.spec['protocol']}")

        # Copy all keys from source to destination store
        if self.spec["protocol"] == "file":
            # For file protocol, copy files directly
            self._copy_store_sync(source_store, dest_path)
        else:
            # For other protocols, use async copying
            import asyncio
            asyncio.run(_copy_zarr_store(source_store, dest_store))

    def _copy_store_sync(self, source_store, dest_path: Path):
        """
        Synchronously copy a Zarr store to local filesystem.
        """
        # Handle different store types
        keys = []

        # For MemoryStore, check for _store_dict first
        if hasattr(source_store, '_store_dict') and isinstance(source_store._store_dict, dict):
            keys = list(source_store._store_dict.keys())
        # Try different methods to get keys from the store
        elif hasattr(source_store, 'list'):
            # Zarr v3 style
            try:
                keys = list(source_store.list())
            except Exception:
                pass
        elif hasattr(source_store, 'keys'):
            # Dict-like interface
            try:
                keys = list(source_store.keys())
            except Exception:
                pass

        if not keys:
            logger.warning(f"Could not get keys from store of type {type(source_store)}")
            return

        logger.debug(f"Found {len(keys)} keys to copy: {keys}")

        for key in keys:
            try:
                # For MemoryStore, access the _store_dict directly
                if hasattr(source_store, '_store_dict') and key in source_store._store_dict:
                    value = source_store._store_dict[key]
                # Try other access methods
                elif hasattr(source_store, 'get'):
                    try:
                        value = source_store.get(key)
                    except Exception:
                        value = None
                else:
                    try:
                        value = source_store[key]
                    except Exception:
                        value = None

                if value is not None:
                    dest_file = dest_path / key
                    dest_file.parent.mkdir(parents=True, exist_ok=True)

                    # Handle Zarr Buffer objects
                    if hasattr(value, 'to_bytes'):
                        # Zarr Buffer object
                        dest_file.write_bytes(value.to_bytes())
                    elif isinstance(value, bytes):
                        dest_file.write_bytes(value)
                    elif hasattr(value, '__bytes__'):
                        dest_file.write_bytes(bytes(value))
                    else:
                        dest_file.write_text(str(value))
                else:
                    logger.warning(f"Could not get value for key '{key}'")

            except Exception as e:
                logger.warning(f"Could not copy key '{key}': {e}")
                continue
    
    def _download_file(self, external_path, download_path):
        if self.spec["protocol"] == "s3":
            self.s3.fget(external_path, download_path)
        elif self.spec["protocol"] == "file":
            safe_copy(external_path, download_path)
        else:
            assert False

    def _upload_buffer(self, buffer, external_path):
        if self.spec["protocol"] == "s3":
            self.s3.put(external_path, buffer)
        elif self.spec["protocol"] == "file":
            safe_write(external_path, buffer)
        else:
            assert False

    def _download_buffer(self, external_path):
        if self.spec["protocol"] == "s3":
            return self.s3.get(external_path)
        if self.spec["protocol"] == "file":
            try:
                return Path(external_path).read_bytes()
            except FileNotFoundError:
                raise errors.MissingExternalFile(
                    f"Missing external file {external_path}"
                ) from None
        assert False

    def _remove_external_file(self, external_path):
        if self.spec["protocol"] == "s3":
            self.s3.remove_object(external_path)
        elif self.spec["protocol"] == "file":
            try:
                Path(external_path).unlink()
            except FileNotFoundError:
                pass

    def exists(self, external_filepath):
        """
        :return: True if the external file is accessible
        """
        if self.spec["protocol"] == "s3":
            return self.s3.exists(external_filepath)
        if self.spec["protocol"] == "file":
            return Path(external_filepath).is_file()
        assert False

    # --- BLOBS ----

    def put(self, data: zarr.Group | zarr.Array) -> uuid.UUID:
        """
        Put the stored values of a zarr array or group into external storage, and insert
        a row into the table to indicate this
        """
        # get the size of all the values in the store
        # Get UUID for this Zarr data
        data_uuid = get_uuid(data)
        
        # Calculate total size of the store
        try:
            size_bytes = data.store.size() if hasattr(data.store, 'size') else 0
        except:
            size_bytes = 0
        
        # Create destination path
        dest_path = self._make_uuid_path(data_uuid)
        
        # Copy the Zarr store to external storage
        self._copy_store(data.store, dest_path)
        
        # Insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size, zarr_path) VALUES (%s, {size}, '{zarr_path}') ON DUPLICATE KEY "
            "UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name, size=size_bytes, zarr_path=str(dest_path)
            ),
            args=(data_uuid.bytes,),
        )
        return data_uuid

    def get(self, data_uuid) -> zarr.Group | zarr.Array | None:
        """
        Get a Zarr group or array from external store.
        """
        if data_uuid is None:
            return None

        # Get the path to the zarr store
        zarr_path = self._make_uuid_path(data_uuid)

        # Create appropriate store based on protocol
        if self.spec["protocol"] == "s3":
            # For S3, use FSSpecStore
            import fsspec
            from zarr.storage import FSSpecStore

            fs = fsspec.filesystem('s3')
            store = FSSpecStore(fs=fs, path=f"{self.spec['bucket']}/{zarr_path}")
        elif self.spec["protocol"] == "file":
            # For file system, use the zarr.open with the directory path
            try:
                # Try direct open first - this is the most compatible approach
                result = zarr.open(str(zarr_path), mode='r')
                return result
            except Exception as e:
                raise MissingExternalFile(f"Cannot open Zarr data at {zarr_path}: {e}")
        else:
            raise ValueError(f"Unsupported protocol: {self.spec['protocol']}")

        # For non-file protocols, try to open using the store
        try:
            # Use zarr.open which is most compatible across versions
            result = zarr.open(store, mode='r')
            return result
        except Exception as e:
            raise MissingExternalFile(f"Cannot open Zarr data at {zarr_path}: {e}")

    # --- ATTACHMENTS ---

    def upload_attachment(self, local_path):
        attachment_name = Path(local_path).name
        uuid = uuid_from_file(local_path, init_string=attachment_name + "\0")
        external_path = self._make_uuid_path(uuid, "." + attachment_name)
        # This method needs to be updated for Zarr stores
        raise NotImplementedError("upload_attachment not yet implemented for Zarr stores")
        # insert tracking info
        self.connection.query(
            """
        INSERT INTO {tab} (hash, size, attachment_name)
        VALUES (%s, {size}, "{attachment_name}")
        ON DUPLICATE KEY UPDATE timestamp=CURRENT_TIMESTAMP""".format(
                tab=self.full_table_name,
                size=Path(local_path).stat().st_size,
                attachment_name=attachment_name,
            ),
            args=[uuid.bytes],
        )
        return uuid

    def get_attachment_name(self, uuid):
        return (self & {"hash": uuid}).fetch1("attachment_name")

    def download_attachment(self, uuid, attachment_name, download_path):
        """save attachment from memory buffer into the save_path"""
        external_path = self._make_uuid_path(uuid, "." + attachment_name)
        self._download_file(external_path, download_path)

    # --- FILEPATH ---

    def upload_filepath(self, local_filepath):
        """
        Raise exception if an external entry already exists with a different contents checksum.
        Otherwise, copy (with overwrite) file to remote and
        If an external entry exists with the same checksum, then no copying should occur
        """
        local_filepath = Path(local_filepath)
        try:
            relative_filepath = str(
                local_filepath.relative_to(self.spec["stage"]).as_posix()
            )
        except ValueError:
            raise DataJointError(
                "The path {path} is not in stage {stage}".format(
                    path=local_filepath.parent, **self.spec
                )
            )
        uuid = uuid_from_buffer(
            init_string=relative_filepath
        )  # hash relative path, not contents
        contents_hash = uuid_from_file(local_filepath)

        # check if the remote file already exists and verify that it matches
        check_hash = (self & {"hash": uuid}).fetch("contents_hash")
        if check_hash.size:
            # the tracking entry exists, check that it's the same file as before
            if contents_hash != check_hash[0]:
                raise DataJointError(
                    f"A different version of '{relative_filepath}' has already been placed."
                )
        else:
            # upload the file and create its tracking entry
            # This method needs to be updated for Zarr stores  
            raise NotImplementedError("upload_filepath not yet implemented for Zarr stores")
            self.connection.query(
                "INSERT INTO {tab} (hash, size, filepath, contents_hash) VALUES (%s, {size}, '{filepath}', %s)".format(
                    tab=self.full_table_name,
                    size=Path(local_filepath).stat().st_size,
                    filepath=relative_filepath,
                ),
                args=(uuid.bytes, contents_hash.bytes),
            )
        return uuid

    def download_filepath(self, filepath_hash):
        """
        sync a file from external store to the local stage

        :param filepath_hash: The hash (UUID) of the relative_path
        :return: hash (UUID) of the contents of the downloaded file or Nones
        """

        def _need_checksum(local_filepath, expected_size):
            limit = config.get("filepath_checksum_size_limit")
            actual_size = Path(local_filepath).stat().st_size
            if expected_size != actual_size:
                # this should never happen without outside interference
                raise DataJointError(
                    f"'{local_filepath}' downloaded but size did not match."
                )
            return limit is None or actual_size < limit

        if filepath_hash is not None:
            relative_filepath, contents_hash, size = (
                self & {"hash": filepath_hash}
            ).fetch1("filepath", "contents_hash", "size")
            external_path = self._make_external_filepath(relative_filepath)
            local_filepath = Path(self.spec["stage"]).absolute() / relative_filepath

            file_exists = Path(local_filepath).is_file() and (
                not _need_checksum(local_filepath, size)
                or uuid_from_file(local_filepath) == contents_hash
            )

            if not file_exists:
                self._download_file(external_path, local_filepath)
                if (
                    _need_checksum(local_filepath, size)
                    and uuid_from_file(local_filepath) != contents_hash
                ):
                    # this should never happen without outside interference
                    raise DataJointError(
                        f"'{local_filepath}' downloaded but did not pass checksum."
                    )
            if not _need_checksum(local_filepath, size):
                logger.warning(
                    f"Skipped checksum for file with hash: {contents_hash}, and path: {local_filepath}"
                )
            return str(local_filepath), contents_hash

    # --- UTILITIES ---

    @property
    def references(self):
        """
        :return: generator of referencing table names and their referencing columns
        """
        return (
            {k.lower(): v for k, v in elem.items()}
            for elem in self.connection.query(
                """
        SELECT concat('`', table_schema, '`.`', table_name, '`') as referencing_table, column_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
        """.format(
                    tab=self.table_name, db=self.database
                ),
                as_dict=True,
            )
        )

    def fetch_external_paths(self, **fetch_kwargs):
        """
        generate complete external filepaths from the query.
        Each element is a tuple: (uuid, path)

        :param fetch_kwargs: keyword arguments to pass to fetch
        """
        fetch_kwargs.update(as_dict=True)
        paths = []
        for item in self.fetch("hash", "attachment_name", "filepath", **fetch_kwargs):
            if item["attachment_name"]:
                # attachments
                path = self._make_uuid_path(item["hash"], "." + item["attachment_name"])
            elif item["filepath"]:
                # external filepaths
                path = self._make_external_filepath(item["filepath"])
            else:
                # blobs
                path = self._make_uuid_path(item["hash"])
            paths.append((item["hash"], path))
        return paths

    def unused(self):
        """
        query expression for unused hashes

        :return: self restricted to elements that are not in use by any tables in the schema
        """
        return self - [
            FreeTable(self.connection, ref["referencing_table"]).proj(
                hash=ref["column_name"]
            )
            for ref in self.references
        ]

    def used(self):
        """
        query expression for used hashes

        :return: self restricted to elements that in use by tables in the schema
        """
        return self & [
            FreeTable(self.connection, ref["referencing_table"]).proj(
                hash=ref["column_name"]
            )
            for ref in self.references
        ]

    def delete(
        self,
        *,
        delete_external_files=None,
        limit=None,
        display_progress=True,
        errors_as_string=True,
    ):
        """

        :param delete_external_files: True or False. If False, only the tracking info is removed from the external
                store table but the external files remain intact. If True, then the external files themselves are deleted too.
        :param errors_as_string: If True any errors returned when deleting from external files will be strings
        :param limit: (integer) limit the number of items to delete
        :param display_progress: if True, display progress as files are cleaned up
        :return: if deleting external files, returns errors
        """
        if delete_external_files not in (True, False):
            raise DataJointError(
                "The delete_external_files argument must be set to either "
                "True or False in delete()"
            )

        if not delete_external_files:
            self.unused().delete_quick()
        else:
            items = self.unused().fetch_external_paths(limit=limit)
            if display_progress:
                items = tqdm(items)
            # delete items one by one, close to transaction-safe
            error_list = []
            for uuid, external_path in items:
                row = (self & {"hash": uuid}).fetch()
                if row.size:
                    try:
                        (self & {"hash": uuid}).delete_quick()
                    except Exception:
                        pass  # if delete failed, do not remove the external file
                    else:
                        try:
                            self._remove_external_file(external_path)
                        except Exception as error:
                            # adding row back into table after failed delete
                            self.insert1(row[0], skip_duplicates=True)
                            error_list.append(
                                (
                                    uuid,
                                    external_path,
                                    str(error) if errors_as_string else error,
                                )
                            )
            return error_list


class ExternalMapping(Mapping):
    """
    The external manager contains all the tables for all external stores for a given schema
    :Example:
        e = ExternalMapping(schema)
        external_table = e[store]
    """

    def __init__(self, schema):
        self.schema = schema
        self._tables = {}

    def __repr__(self):
        return "External file tables for schema `{schema}`:\n    ".format(
            schema=self.schema.database
        ) + "\n    ".join(
            '"{store}" {protocol}:{location}'.format(store=k, **v.spec)
            for k, v in self.items()
        )

    def __getitem__(self, store):
        """
        Triggers the creation of an external table.
        Should only be used when ready to save or read from external storage.

        :param store: the name of the store
        :return: the ExternalTable object for the store
        """
        if store not in self._tables:
            self._tables[store] = ExternalZarrTable(
                connection=self.schema.connection,
                store=store,
                database=self.schema.database,
            )
        return self._tables[store]

    def __len__(self):
        return len(self._tables)

    def __iter__(self):
        return iter(self._tables)


async def _copy_zarr_store(source_store: zarr.abc.store.Store, dest_store: zarr.abc.store.Store) -> None:
    """Copy the contents of a Zarr store using list_dir and set. This is a brittle, temporary
    implementation that should be made more robust to handle the failure of individual keys
    to copy.
    """

    # Handle different store types and their APIs
    try:
        # For newer Zarr v3 stores
        if hasattr(source_store, 'list_dir'):
            keys = []
            async for key in source_store.list_dir(prefix=""):
                keys.append(key)
        else:
            # For older stores or different implementations
            if hasattr(source_store, 'keys'):
                keys = list(source_store.keys())
            elif hasattr(source_store, 'listdir'):
                keys = source_store.listdir()
            else:
                # For dict-like stores (MemoryStore)
                keys = list(source_store)
    except Exception as e:
        # Fallback for different store implementations
        try:
            keys = list(source_store.keys()) if hasattr(source_store, 'keys') else list(source_store)
        except:
            keys = list(source_store)

    for key in keys:
        try:
            # Get value from source
            if hasattr(source_store, 'get') and hasattr(source_store.get, '__aenter__'):
                # Async get
                value = await source_store.get(key, prototype=zarr.core.buffer.default_buffer_prototype())
            elif hasattr(source_store, 'get'):
                # Sync get for dict-like stores
                value = source_store[key] if key in source_store else source_store.get(key, None)
            else:
                value = source_store[key]

            if value is not None:
                # Set value in destination
                if hasattr(dest_store, 'set') and hasattr(dest_store.set, '__aenter__'):
                    # Async set
                    await dest_store.set(key, value)
                else:
                    # Sync set for dict-like stores
                    dest_store[key] = value

        except Exception as e:
            # Skip keys we can't copy but log the issue
            logger.warning(f"Could not copy key '{key}': {e}")
            continue