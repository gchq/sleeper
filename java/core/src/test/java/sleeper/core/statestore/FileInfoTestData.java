/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.core.statestore;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.time.Instant;
import java.util.stream.Collectors;

public class FileInfoTestData {
    private FileInfoTestData() {
    }

    public static final long DEFAULT_NUMBER_OF_RECORDS = 100L;

    private static final Schema DEFAULT_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .build();

    public static FileInfo defaultFileOnRootPartition(String filename) {
        return defaultFileOnRootPartitionWithRecords(filename, DEFAULT_NUMBER_OF_RECORDS);
    }

    public static FileInfo defaultFileOnRootPartitionWithRecords(String filename, long records) {
        return FileInfo.builder()
                .rowKeyTypes(DEFAULT_SCHEMA.getRowKeyTypes())
                .filename(filename).partitionId("root")
                .numberOfRecords(records).fileStatus(FileInfo.FileStatus.ACTIVE)
                .lastStateStoreUpdateTime(Instant.parse("2022-12-08T11:03:00.001Z"))
                .build();
    }

    public static FileInfo defaultPartitionSingleFileWithRecords(Partition partition, long records) {
        return FileInfo.builder()
                .rowKeyTypes(partition.getRowKeyTypes())
                .filename(partition.getId() + ".parquet").partitionId(partition.getId())
                .numberOfRecords(records).fileStatus(FileInfo.FileStatus.ACTIVE)
                .lastStateStoreUpdateTime(Instant.parse("2022-12-08T11:03:00.001Z"))
                .build();
    }

    private static Key minRowKey(Partition partition) {
        return Key.create(partition.getRegion().getRanges().stream()
                .map(Range::getMin)
                .collect(Collectors.toList()));
    }

    private static Key maxRowKey(Partition partition) {
        return Key.create(partition.getRegion().getRanges().stream()
                .map(Range::getMax)
                .collect(Collectors.toList()));
    }
}
