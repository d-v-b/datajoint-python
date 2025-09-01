# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "zarr",
#   "sqlalchemy",
# ]
# ///
from tempfile import TemporaryDirectory
import zarr
import numpy as np
from sqlalchemy import create_engine, JSON
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, Session
from pprint import pprint
class Base(DeclarativeBase):
    ...

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)

class ZarrRef(Base):
    """
    Model of a table in a SQL database to store informed references to Zarr arrays.
    """
    __tablename__ = "zarr_ref"
    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column()
    meta: Mapped[JSON] = mapped_column(JSON)

    def __repr__(self) -> str:
        return f"ZarrRef(id={self.id}, url={self.url}, meta={self.meta})"

# set up DB
Base.metadata.create_all(engine)

with TemporaryDirectory() as tmpdir:
    store_url = 'file://' + tmpdir
    zg = zarr.create_group(store=tmpdir, attributes={"description": "example zarr group"})
    zg.create_array(data=np.array([[1,2,3],[4,5,6]]), chunks=(2,3), name='data', attributes={"description": "example zarr array"})

    meta_tree = {k: v.metadata.to_dict() for k,v in zg.members()} | {"": zg.metadata.to_dict()}

    with Session(engine) as sess:
        zarr_ref = ZarrRef(url=store_url, meta=meta_tree)
        print(zarr_ref.id)
        sess.add(zarr_ref)
        sess.commit()
        print(sess.get(ZarrRef, 1))



