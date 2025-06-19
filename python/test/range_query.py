#  Copyright 2022-2025 Crown Copyright
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import argparse
import json

from pq.parquet_serial import ParquetSerialiser
from sleeper.client import SleeperClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run range query against Sleeper")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--queryid", required=True)
    parser.add_argument("--query", type=json.loads, required=True)
    parser.add_argument("--outdir", required=True)

    args = parser.parse_args()

    # Setting use_threads as a workaround for the following bug in Arrow:
    # https://github.com/apache/arrow/issues/34314
    sleeper_client = SleeperClient(args.instance, use_threads=False)
    records = sleeper_client.range_key_query(args.table, [args.query], args.queryid)

    with open(args.outdir + "/" + args.queryid + ".txt", "wb") as file:
        writer = ParquetSerialiser(file)
        for record in records:
            writer.write_record(record)
        writer.write_tail()
