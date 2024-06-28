import sqlalchemy_adbc_flight_sql_driver
from sqlalchemy import create_engine, MetaData, Table, select, Column, DateTime, func, text, Integer, String, Sequence
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session
import logging

# Setup logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


Base = declarative_base()


class FakeModel(Base):  # type: ignore
    __tablename__ = "fake"

    id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)
    name = Column(String)


# Constants
SQLALCHEMY_DATABASE_URL="adbc_flight_sql://flight_username:flight_password@localhost:31337?disableCertificateVerification=True&useEncryption=True"


def main():

    engine = create_engine(url=SQLALCHEMY_DATABASE_URL)
    Base.metadata.create_all(bind=engine)

    metadata = MetaData()
    metadata.reflect(bind=engine)

    for table_name in metadata.tables:
        print(f"Table name: {table_name}")

    with Session(bind=engine) as session:

        # Execute some raw SQL
        results = session.execute(statement=text("SELECT * FROM joe")).fetchall()
        print(results)

        # Try a SQLAlchemy table select
        joe: Table = metadata.tables["joe"]
        stmt = select(joe.c.a)

        results = session.execute(statement=stmt).fetchall()
        print(results)

        # Try ORM
        session.add(FakeModel(name="Joe"))
        session.commit()

        joe = session.query(FakeModel).filter(FakeModel.name == "Joe").first()

        assert joe.name == "joe"


if __name__ == "__main__":
    main()
