/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.bulkimport.runner.common;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * A reference to a sketch file written during a Spark job. Used to calculate split points when pre-splitting
 * partitions.
 *
 * @param partitionId the ID of the Sleeper partition that the sketch file covers
 * @param sketchBytes the sketch file held as a serialised byte array
 */
public record SparkSketchBytesRow(String partitionId, byte[] sketchBytes) {

    public static final String PARTITION_FIELD_NAME = "__partition";
    public static final String FILE_BYTE_ARRAY = "__sketchByteArray";

    /**
     * Reads a Spark row containing a reference to a sketch file.
     *
     * @param  sparkRow the Spark row
     * @return          the reference to the sketch file held in the row
     */
    public static SparkSketchBytesRow from(Row sparkRow) {
        return new SparkSketchBytesRow(sparkRow.getString(0), (byte[]) sparkRow.get(1));
    }

    /**
     * Builds a Spark row containing this reference.
     *
     * @return the Spark row
     */
    public Row toSparkRow() {
        return RowFactory.create(partitionId, sketchBytes);
    }

    /**
     * Creates a Spark schema for rows containing references of this type.
     *
     * @return the schema
     */
    public static StructType createSchema() {
        return new StructType()
                .add(PARTITION_FIELD_NAME, DataTypes.StringType)
                .add(FILE_BYTE_ARRAY, DataTypes.BinaryType);
    }
}
