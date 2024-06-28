import os
import logging

from sqlalchemy import create_engine, MetaData, Table, select, Column, text, Integer, String, Sequence
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy.engine.url import URL

# Setup logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


Base = declarative_base()


class FakeModel(Base):  # type: ignore
    __tablename__ = "fake"

    id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)
    name = Column(String)


def main():
    # Build the URL
    url = URL(drivername="flight_sql",
              host="localhost",
              port=31337,
              database="default",
              username=os.getenv("FLIGHT_USERNAME", "flight_username"),
              password=os.getenv("FLIGHT_PASSWORD", "flight_password"),
              query={"disableCertificateVerification": "True", "useEncryption": "True"},
              )

    engine = create_engine(url=url)
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
