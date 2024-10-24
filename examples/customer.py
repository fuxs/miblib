# Copyright 2024 Michael Bungenstock
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import datetime, timedelta
from mib import bq

""" 
Please run the following SQL in BigQuery before executing this example. See
README.md for more details:

-- Optional: create a dataset in the default location or append
-- OPTIONS(location="us-central1")

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
"""

def data() -> list[dict]:    
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
    return table


def main() -> None:
    table = data()
    # `demo_ds.customer` is using the default project
    # `my_project.demo_ds.customer` is the fully qualified name (recommended)
    # this is a batch upload
    with bq.BQBatchWriter(table_id="demo_ds.customer") as bw:
        bw.append(*table)

    # this class supports streaming for immedidate access
    with bq.BQStreamWriter(table_id="demo_ds.customer") as bw:
        bw.send(*table)

if __name__ == "__main__":
    main()
