from sqlalchemy import Column, Integer, String, Sequence
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class FakeModel(Base):  # type: ignore
    __tablename__ = "fake"

    id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)
    name = Column(String)
