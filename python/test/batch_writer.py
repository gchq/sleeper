#  Copyright 2022-2023 Crown Copyright
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

from pq.parquet_deserial import ParquetDeserialiser
from sleeper.sleeper import SleeperClient


def read_files(*files: str):
    parq = ParquetDeserialiser()
    file_records = []
    for fileName in files:
        with open(fileName, mode="rb") as file:
            reader = parq.read(file)
            for r in reader:
                file_records.append(r)
    return file_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write files to Sleeper")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--files", nargs="+")

    args = parser.parse_args()

    sleeper_client = SleeperClient(args.instance)
    table_name = args.table

    records = read_files(args.files)
    with sleeper_client.create_batch_writer(table_name) as writer:
        writer.write(records)
