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
import json
import random
from typing import BinaryIO, Mapping, List, Dict

import pyarrow as pa
import pyarrow.parquet as pq

ROW_GROUP_SIZE = 128 * 1024 * 1024
"""Maximum in memory size of row group."""


def _create_writer(stream: BinaryIO, record: Mapping[str, str]) -> pq.ParquetWriter:
    """
    Creates a Parquet writer based on the schema of the first record.

    We create a single row table which is just used to infer the schema.
    :param stream: the binary output stream
    :param record: the first record to get the schema from
    :return: a parquet writer
    """
    # We create a simple table from the first given row to infer the Parquet schema
    # Wrap values in singleton list
    list_value_dict: Dict[str, List[str]] = {k: [v] for k, v in record.items()}
    table: pa.Table = pa.Table.from_pydict(list_value_dict)

    # Inferred schema in table above will have every column set to nullable (optional)
    # We don't want that, so we duplicate the schema below and make all columns non-nullable (required)
    original_schema: pa.Schema = table.schema

    fields = [original_schema.field(i)
              for i in range(len(original_schema.names))]
    non_optional_fields = _non_nullable(fields)

    # Create the new schema
    replacement: pa.Schema = pa.schema(non_optional_fields)

    # Create the Parquet reader
    writer = pq.ParquetWriter(stream, replacement)
    return writer


def _non_nullable(fields: List[pa.Field], deep: bool = True) -> List[pa.Field]:
    """
    Converts a list of Arrow Fields into non-nullable (i.e. required, not optional)
    fields. This function will recurse down into nested types (List, Struct) if 
    deep is True. Otherwise, a shallow copy will result where lists and nested
    types may contain optional fields.

    :param fields: list of fields to make required
    :param deep: whether to produce a deep copy
    :return: new fields that are required
    """
    new_fields = []
    for field in fields:
        if type(field.type) == pa.StructType and deep:
            struct_fields = [field.type[i]
                             for i in range(field.type.num_fields)]
            # Recurse
            new_struct_fields = _non_nullable(struct_fields)
            new_fields.append(pa.field(field.name, pa.struct(
                new_struct_fields), False, metadata=field.metadata))
        elif type(field.type) == pa.ListType and deep:
            # Recurse
            list_type = [field.type.value_field]
            new_list_type = _non_nullable(list_type)
            new_fields.append(pa.field(field.name, pa.list_(
                new_list_type[0]), False, metadata=field.metadata))
        else:
            new_fields.append(pa.field(field.name, field.type,
                                       False, metadata=field.metadata))
    return new_fields


class ParquetSerialiser():
    """
    Class that can serialise to Parquet.
    """

    def _get_next_mem_sample_duration(self) -> int:
        """
        Get the number of rows to wait before next taking a memory sample.

        :return: number of records to wait before sampling the record memory size
        """
        sam: int = random.randint(20, 30)
        return sam

    def __init__(self, file: BinaryIO):
        self._file: BinaryIO = file
        self._writer: pq.ParquetWriter = None
        self._clear_table()

    def _clear_table(self) -> None:
        """
        Empty the data buffer for new rows being written.
        """
        self._buffer: Dict = dict()
        self._row_count: int = 0
        self._total_memory: int = 0
        self._mem_sample_duration: int = self._get_next_mem_sample_duration()
        self._next_row_memory_check: int = self._row_count + self._mem_sample_duration

    def _flush_table(self) -> None:
        """
        Flushes the current data buffer of rows as a row group to the parquet file.
        """
        if self._writer is not None:
            # If we have a writer and some rows then flush
            if self._row_count > 0:
                self._writer.write_table(pa.Table.from_pydict(
                    self._buffer, schema=self._writer.schema))
                self._clear_table()

    def _write_row(self, record: Mapping[str, str]) -> None:
        """
        Enter the given record into the data buffer.

        :param record: the record to write
        """
        for k, v in record.items():
            self._buffer.setdefault(k, list()).append(v)

        self._row_count += 1

    def _getsize(self, record: Mapping[str, str]) -> int:
        """
        A hacky way to estimate the size of the object.
        
        :param record: the record to estimate the size of
        """
        record_in_json = json.dumps(record)
        return len(record_in_json)

    def write_record(self, record: Mapping[str, str]) -> None:
        """
        Write to a parquet file. On provision of the first record, the key names in the record
        are used as header fields.

        :param record: the record to write to the stream
        """
        # If we haven't written the header yet, we need to do that
        if self._writer is None:
            self._writer = _create_writer(self._file, record)

        # Add this to the internal buffer
        self._write_row(record)

        # We sample the byte size of a row being written every N rows, then assume that the previous N rows
        # were all that size, when that buffer estimate is above the row group size, we flush the table.
        # This tends to over-estimate quite badly (especially given compression) so we have a scaling factor.

        if self._row_count >= self._next_row_memory_check:
            row_sz: int = self._getsize(record)
            # This tends to massively over-estimate, so we scale down by a scaling factor
            total_predicted: int = (row_sz * self._mem_sample_duration) / 5
            self._total_memory += total_predicted
            if self._total_memory >= ROW_GROUP_SIZE:
                # Total memory estimate exceeds row group size
                self._flush_table()
            else:
                # Room to grow, re-calculate sample duration
                self._mem_sample_duration = self._get_next_mem_sample_duration()
                self._next_row_memory_check = self._row_count + self._mem_sample_duration

    def write_tail(self) -> None:
        """
        Flush any remaining records and then flush the stream.
        """
        if not self._writer:
            # No records written!
            self.write_record({})

        self._flush_table()
        self._writer.close()
        self._file.flush()
