/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.core.partition;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * A test helper for creating partitions based on split points.
 */
public class SplitPointsTestHelper {

    private SplitPointsTestHelper() {
    }

    /**
     * Creates a partition tree by finding records at the boundaries. This can be used when we are generating predefined
     * test data, and we can generate the nth record in sort order at will. This will create a leaf partition for each
     * group of records where we specify the number of records in a group. The boundaries of each partition will be the
     * value of the split field for the record at the boundary. This will take the first row key as the field to split
     * on. This may be used to avoid needing to hold all the test data in memory at once.
     *
     * @param  recordsPerPartition the number of records to fit in each partition
     * @param  totalRecords        the total number of records that will be in the table
     * @param  generator           a method to find the nth record in sort order
     * @param  schema              the schema
     * @return                     the partition tree
     */
    public static PartitionTree createPartitionTreeWithRecordsPerPartitionAndTotal(int recordsPerPartition, int totalRecords, NthRecordGenerator generator, Schema schema) {
        List<Object> splitPoints = new ArrayList<>();
        Field splitField = schema.getRowKeyFields().get(0);
        for (int i = recordsPerPartition; i < totalRecords; i += recordsPerPartition) {
            splitPoints.add(generator.getNthRecord(i).get(splitField.getName()));
        }
        return PartitionsFromSplitPoints.treeFrom(schema, splitPoints);
    }

    /**
     * A generator for the nth record in sort order to generate test data on the fly. Used when the test data is defined
     * deterministically to avoid needing to hold it all in memory at once.
     */
    @FunctionalInterface
    public interface NthRecordGenerator {

        /**
         * Gets the nth record in the test data in sort order.
         *
         * @param  n the index of the record, starting from 0
         * @return   the record
         */
        Record getNthRecord(int n);
    }
}
