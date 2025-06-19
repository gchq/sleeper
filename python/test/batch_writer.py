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

from pyarrow.parquet import ParquetFile

from pq.parquet_deserial import ParquetDeserialiser
from sleeper.client import SleeperClient


def read_files(filename: str):
    parq = ParquetDeserialiser()
    file_records = []
    with open(filename, "rb") as file:
        with ParquetFile(file) as po:
            for record in parq.read(po):
                file_records.append(record)
    return file_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write files to Sleeper")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--jobid", required=True)
    parser.add_argument("--file", required=True)

    args = parser.parse_args()

    sleeper_client = SleeperClient(args.instance)
    table_name = args.table

    records = read_files(args.file)
    with sleeper_client.create_batch_writer(table_name, args.jobid) as writer:
        writer.write(records)
