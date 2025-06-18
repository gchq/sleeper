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
from typing import Generator, Mapping

import pyarrow as pa
from pyarrow.parquet import ParquetFile

ROW_GROUP_MAX_SIZE = 256 * 1024 * 1024
"""Maximum size of row group (compressed) we will try to read."""


class ParquetDeserialiser:
    def __init__(self, use_threads=True):
        self.use_threads = use_threads

    def read(self, file: ParquetFile) -> Generator[Mapping[str, str], None, None]:
        """
        Iterate through every record in a Parquet file.

        Each record is then returned as a dictionary of header names to values.

        :param file: the ParquetFile to read from
        :return: generator of records
        """
        pmd = file.metadata
        # Iterate over row groups in file
        num_groups: int = file.num_row_groups
        for i in range(num_groups):
            # Check that we can read a row group this size
            row_group_size: int = pmd.row_group(i).total_byte_size
            if row_group_size > ROW_GROUP_MAX_SIZE:
                raise RuntimeError(f"Parquet row group {i} is too big, total bytes {row_group_size}")

            group: pa.Table = file.read_row_group(i, use_threads=self.use_threads)
            # Iterate each page
            for batch in group.to_batches():
                columns = batch.to_pydict()
                # Yield row
                for j in range(batch.num_rows):
                    yield {k: v[j] for k, v in columns.items()}
