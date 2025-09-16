import os

import numpy as np
import zarr
from numpy.testing import assert_array_equal

import datajoint as dj
from datajoint._zarr import ExternalZarrTable

from .schema_external import Simple, SimpleRemote


def test_put(schema_ext, mock_stores, mock_cache):
    """
    Test that a Zarr group with an array can be inserted into the database via 
    a put operation on the Zarr table object
    Test also that the hash returned by the put operation can be used to query the 
    Zarr table object.
    Test that the zarr group returned by fetching the zarr table object with the hash
    has the same contents as the original.
    """
    zarrt = ExternalZarrTable(
        schema_ext.connection, 
        store="raw", 
        database=schema_ext.database
    )
    
    # Create a Zarr group with an array
    zstore = {}
    zgroup = zarr.create_group(store=zstore)
    test_data = np.arange(10)
    zgroup.create_array(name='test_array', data=test_data)
    
    # Put the Zarr group into storage
    hash1 = zarrt.put(zgroup)

    # Fetch the hash from the database
    fetched_hashes = zarrt.fetch("hash")
    assert len(fetched_hashes) > 0
    assert hash1.bytes in [h.bytes for h in fetched_hashes]

    # Retrieve the Zarr group using the hash
    output = zarrt.get(hash1)

    assert isinstance(output, zarr.Group)
    for key, value in zgroup.members(max_depth=None):
        assert key in output
        assert output.get(key).metadata == value.metadata

        # Verify the actual data content for arrays
        if isinstance(value, zarr.Array):
            assert_array_equal(output[key][:], value[:])


def test_put_array(schema_ext, mock_stores, mock_cache):
    """
    Test that a single Zarr array (not group) can be stored and retrieved.
    """
    zarrt = ExternalZarrTable(
        schema_ext.connection,
        store="raw",
        database=schema_ext.database
    )

    # Create a standalone Zarr array
    test_data = np.random.random((10, 5))
    zarray = zarr.create_array(data=test_data, store={})

    # Put the Zarr array into storage
    hash1 = zarrt.put(zarray)

    # Retrieve the Zarr array using the hash
    output = zarrt.get(hash1)

    assert isinstance(output, zarr.Array)
    assert_array_equal(output[:], test_data)
    