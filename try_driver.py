import os
import logging

from sqlalchemy import create_engine, MetaData, Table, select, Column, text, Integer, String, Sequence
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

# Setup logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


Base = declarative_base()


class FakeModel(Base):  # type: ignore
    __tablename__ = "fake"

    id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)
    name = Column(String)


# Constants
SQLALCHEMY_DATABASE_URL="flight_sql://flight_username:flight_password@localhost:31337?disableCertificateVerification=True&useEncryption=True"


def main():

    engine = create_engine(url=SQLALCHEMY_DATABASE_URL,
                           # username=os.getenv("FLIGHT_USERNAME", "flight_username"),
                           # password=os.getenv("FLIGHT_PASSWORD", "flight_password")
                           )
    Base.metadata.create_all(bind=engine)

    metadata = MetaData()
    metadata.reflect(bind=engine)

    for table_name in metadata.tables:
        print(f"Table name: {table_name}")

    with Session(bind=engine) as session:

        # Try ORM
        session.add(FakeModel(id=1, name="Joe"))
        session.commit()

        joe = session.query(FakeModel).filter(FakeModel.name == "Joe").first()

        assert joe.name == "Joe"

        # Execute some raw SQL
        results = session.execute(statement=text("SELECT * FROM fake")).fetchall()
        print(results)

        # Try a SQLAlchemy table select
        fake: Table = metadata.tables["fake"]
        stmt = select(fake.c.name)

        results = session.execute(statement=stmt).fetchall()
        print(results)


if __name__ == "__main__":
    main()
