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
package sleeper.bulkimport.job.runner;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import sleeper.core.statestore.FileInfo;

import java.time.Instant;

public class SparkFileInfoRow {

    private SparkFileInfoRow() {
    }

    public static final String PARTITION_FIELD_NAME = "__partition";
    public static final String FILENAME_FIELD_NAME = "__fileName";
    public static final String NUM_RECORDS_FIELD_NAME = "__numRecords";

    public static FileInfo createFileInfo(Row row) {
        return FileInfo.builder()
                .filename(row.getAs(FILENAME_FIELD_NAME))
                .jobId(null)
                .lastStateStoreUpdateTime(Instant.now())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(row.getAs(PARTITION_FIELD_NAME))
                .numberOfRecords(row.getAs(NUM_RECORDS_FIELD_NAME))
                .build();
    }

    public static StructType createFileInfoSchema() {
        return new StructType()
                .add(PARTITION_FIELD_NAME, DataTypes.StringType)
                .add(FILENAME_FIELD_NAME, DataTypes.StringType)
                .add(NUM_RECORDS_FIELD_NAME, DataTypes.LongType);
    }
}
