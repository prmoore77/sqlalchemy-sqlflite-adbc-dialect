# SQLAlchemy Flight SQL ADBC Driver 

Basic SQLAlchemy driver for [Flight SQL Server](https://github.com/voltrondata/flight-sql-server-example)

## Installation
```sh
$ pip install sqlalchemy-adbc-flight-sql-driver
```

## Usage

Once you've installed this package, you should be able to just use it, as SQLAlchemy does a python path search

```python
import sqlalchemy_adbc_flight_sql_driver
from sqlalchemy import create_engine, MetaData, Table, select, Column, DateTime, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

# Constants
SQLALCHEMY_DATABASE_URL="adbc_flight_sql://flight_username:flight_password@localhost:31337?disableCertificateVerification=True&useEncryption=True"


def main():
    engine = create_engine(url=SQLALCHEMY_DATABASE_URL)

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


if __name__ == "__main__":
    main()
```
