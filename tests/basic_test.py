from sqlalchemy import Table, select, text
from classes import FakeModel


def test_sqlalchemy_basics(session_and_metadata):
    session, metadata = session_and_metadata

    # Try ORM
    session.add(FakeModel(id=1, name="Joe"))
    session.commit()

    print(f"Fake table contents: {session.execute(text("SELECT * FROM fake")).fetchall()}")

    joe = session.query(FakeModel).filter(FakeModel.name == "Joe").first()

    assert joe is not None
    assert joe.name == "Joe"

    # Execute some raw SQL
    results = session.execute(statement=text("SELECT * FROM fake")).fetchall()
    print(results)

    # Try a SQLAlchemy table select
    fake: Table = metadata.tables["fake"]
    stmt = select(fake.c.name)

    results = session.execute(statement=stmt).fetchall()
    print(results)
    
    assert results is not None
