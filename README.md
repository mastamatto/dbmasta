# dbConnect (simple mariadb interface)

## Overview

This Python package provides a simple interface for interacting with MariaDB databases using SQLAlchemy Core. It abstracts some common database operations into more manageable Python methods, allowing for easy database queries, inserts, updates, and deletes.

## Installation

To install this package, run the following pip command. **Note: this requires SQLAlchemy 2.0.27 or greater**

```bash
pip install dbConnect
```

Replace `yourusername/yourrepository` with the actual path to your repository.

## Basic Usage

### Configuration

First, configure the database client with the necessary credentials:

```python
from dbConnect import DataBase, AsyncDataBase

# Initialize the database client
db = DataBase(
    dict(
        username='username', 
        password='password', 
        host='host', 
        port=3306, 
        database_name='database_name'
        )
    )
# Async Version
db = AsyncDataBase(
    dict(
        username='username', 
        password='password', 
        host='host', 
        port=3306, 
        database_name='database_name'
        )
    )


# Initiliaze using environment variables
db = DataBase.env()
# Async Version
db = AsyncDataBase.env()
```

### Executing Queries

You can execute a simple SELECT query to fetch data:

```python
import datetime as dt

# Create parameters
params = {
    "date": db.before(dt.date(2024,1,1), inclusive=True)
}

# Execute the query
dbr = db.select("database", "table", params)

# Examine the results
if dbr.successful:
    print(dbr.records)
else:
    print(dbr.error_info)
```

### Complex Queries

The following query would generate this text:

```python
import datetime as dt

# Create parameters
params = {
    "_OR_": db.or_(
        [
            {"date": db.after(dt.date(2020,1,1)), "category": "sales"},
            {"date": db.before(dt.date(2020,1,1)), "category": db.not_(db.in_, ["purchases","adjustments","sales"])},
        ]
    ),
    "_AND_": db.and_(
        [
            {"keyfield": db.starts_with("SJ")},
            {"keyfield": db.not_(db.ends_with("2E"))}
        ]
    )
    "status": "under_review"
}

# Execute the query
dbr = db.select("database", "table", params)

# Examine the results
if dbr.successful:
    print(dbr.records)
else:
    print(dbr.error_info)
```

The raw text of the query can be retrieved from the attribute `dbr.raw_query` from the DataBaseResponse object,
which the DataBase.select method returns. The text in the above example would be as follows:

```sql
SELECT * FROM `database`.`table`
WHERE ((`date` > '2020-01-01' and `category`='sales') or 
(`date` < '2020-01-01' and `category` not in ('sales')))
AND `keyfield` LIKE 'SJ%' AND `keyfield` NOT LIKE '%2E'
AND `status`='under_review';
```

Or in simple terms...
Get all records `under_review` where the keyfield starts with `SJ`, but doesn't end with `2E`. Pull these if either:

- dated after `2020-01-01` and categorized as a `sale`
- dated before `2020-01-01` and not categorized as `sale`,`purchase` or `adjustment`.


### Result Modification from `DataBase.select`

In addition to complex conditions for filtering records, you can:

- sort records

    ```pythoN
    db.select(..., order_by="column_name", reverse=True)
    ```

- limit and offset results

    ```python
    # for offset pagination
    db.select(..., limit=100, offset=0)
    ```

- filter columns

    ```python
    # only receive the data for the fields you provide
    db.select(..., columns=["keyfield", "name", "date"])
    ```

- get textual output (without executing)

    ```python
    # this will not execute the query, but will return the raw query needed to execute
    raw_textual_query = db.select(..., textual=True)
    print(raw_textual_query)
    new_query = f"INSERT INTO `filteredtable` ({raw_textual_query[:-1]});
    dbr = db.run(new_query)
    ```

- get model output by providing a model factory

    ```python
    from pydantic import BaseModel
    import datetime as dt

    class Record(BaseModel):
        keyfield: str
        date: dt.date
        status: str

    model_factory = lambda row: Record(**row)

    # only receive the data for the fields you provide
    dbr = db.select(..., response_model=model_factory)
    # each record will be an instance of Record
    ```