# SQLAlchemy Flight SQL ADBC Dialect 

Basic SQLAlchemy dialect for the [Flight SQL Server Example](https://github.com/voltrondata/flight-sql-server-example)

## Installation

### Option 1 - from PyPi
```sh
$ pip install sqlalchemy-flight-sql-adbc-dialect
```

### Option 2 - from source - for development
```shell
git clone https://github.com/prmoore77/sqlalchemy_flight_sql_adbc_dialect.git

cd sqlalchemy_flight_sql_adbc_dialect

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install SQLAlchemy Flight SQL ADBC Dialect - in editable mode with dev dependencies
pip install --editable .[dev]
```

### Note
For the following commands - if you running from source and using `--editable` mode (for development purposes) - you will need to set the PYTHONPATH environment variable as follows:
```shell
export PYTHONPATH=$(pwd)/src
```

## Usage

Once you've installed this package, you should be able to just use it, as SQLAlchemy does a python path search

```python
from sqlalchemy import create_engine, MetaData, Table, select, Column, DateTime, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

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
