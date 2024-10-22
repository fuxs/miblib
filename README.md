# miblib

This is a collection of useful Python functions and classes.

## Installation

```bash
pip install git+https://github.com/fuxs/miblib.git
```

## BQBatchWriter and BQStreamWriter

The class ```mib.bq.BQBatchWriter``` expects one Python dictionary per row,
where the keys must match the column names. The values will be converted from
the recommended Python type to a serializable target type. It is possible to use
custom type mappings and custom converters during the creation of the
```mib.bq.BQBatchWriter``` object. Take a look at the following table for the
default settings:

| BQ Type  | Recommended Type | Target Type | Default Converter |
|----------|------------------|-------------|-------------------|
| ARRAY | ```list```| - | - |
| RECORD | ```dict```| - | - |
| STRING   | ```str```   |```str```   | ```lambda v: str(v)```|
| BOOL     | ```bool```  |```bool```   | ```lambda v: bool(v)```|
| BOOLEAN  | ```bool```  |```bool```   | ```lambda v: bool(v)```|
| INTEGER  | ```int```   |```int```   | ```lambda v: int(v)```|
| INT64    | ```int```   |```int```   | ```lambda v: int(v)```|
| FLOAT    | ```float``` |```float```   | ```lambda v: float(v)```|
| FLOAT64  | ```float``` |```float```   | ```lambda v: float(v)```|
| TIME     | ```datetime.time```|```str```   | ```lambda v: v.strftime("%T.%f")```|
| DATETIME | ```datetime.datetime```|```str```   | ```lambda v: v.isoformat( sep=" ", timespec="milliseconds")```|
| DATE"    | ```datetime.date```|```int```   | ```lambda v: (v - dt.datetime(1970, 1, 1)).days```|
| TIMESTAMP| ```datetime.time```|```int```   | ```lambda v: int((v.timestamp() * 1_000_000))```|
| JSON     | ```str```   |```str```   | ```lambda v: str(v)```|
| GEOGRAPHY| ```str```   |```str```   | ```lambda v: str(v)```|
| NUMERIC  | ```decimal.Decimal```|```str```   | ```lambda v: str(round(v, 9))```|
| BIGNUMERIC|```decimal.Decimal```|```str```   | ```lambda v: str(round(v, 38))```|

### Simple example

First, let us create a simple table in BigQuery. Execute the following SQL code in BigQuery to create a simple customer table supporting multiple addresses in repeated records (```ARRAY<STRUCT<>>```):

[Go to BigQuery](https://console.cloud.google.com/bigquery)

```SQL
-- Optional: create a dataset in the default location or append  OPTIONS(location="us-central1")
CREATE SCHEMA IF NOT EXISTS `demo_ds`;

-- Create a table with the following schema
CREATE TABLE `demo_ds.customer` (
    first_name STRING,
    last_name  STRING,
    creation TIMESTAMP,
    addresses ARRAY<
        STRUCT<
            address STRING,
            city STRING,
            zip STRING
        >
    >,
    member BOOL
);
```

A matching Python object could now look like this:

```Python
from datetime import datetime, timedelta

row1 = {
    "first_name": "Per",
    "last_name": "Wolter",
    "creation": datetime.now()-timedelta(days=5),
    "addresses": [
        {"address": "Sackgasse 11",
        "city": "Quartzburg",
        "zip": "45576"},
        {"address": "Belzeweg 16a",
        "city": "Runken",
        "zip": "20043"},
    ],
    "member": True
}
```

Let's create a second row and combine it to list:

```Python
row2 = {
    "first_name": "Daniela",
    "last_name": "Quant",
    "creation": datetime.now()-timedelta(days=10),
    "addresses": [
        {"address": "Twiete 5",
        "city": "Golln",
        "zip": "73451"},
    ],
    "member": False
}

table = [row1, row2]
```

The ```BQBatchWriter``` is a context manager and we use the append method to
send the two rows to BigQuery:

```Python
from mib import bq

with bq.BQBatchWriter(table_id="demo_ds.customer") as bw:
        bw.append(*table)
```

This class uses an application-created stream of type *pending* to implemented
batch streaming, it is an alternative to BigQuery load jobs. You can find more
information
[here](https://cloud.google.com/bigquery/docs/write-api#pending_type).

If you want to stream your data with immediate access in BigQuery then use the
class ```BQStreamWriter``` instead:

```Python

with bq.BQStreamWriter(table_id="demo_ds.customer") as bw:
        bw.append(*table)
```

You can find the complete example code in the file
[customer.py](examples/customer.py).
