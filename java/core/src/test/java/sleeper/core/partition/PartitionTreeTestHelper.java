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
package sleeper.core.partition;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * A test helper for creating partitions based on split points.
 */
public class PartitionTreeTestHelper {

    private PartitionTreeTestHelper() {
    }

    /**
     * Creates a partition tree by finding rows at the boundaries. This can be used when we are generating predefined
     * test data, and we can generate the nth row in sort order at will. This will create a leaf partition for each
     * group of rows where we specify the number of rows in a group. The boundaries of each partition will be the
     * value of the split field for the row at the boundary. This will take the first row key as the field to split
     * on. This may be used to avoid needing to hold all the test data in memory at once.
     *
     * @param  rowsPerPartition the number of rows to fit in each partition
     * @param  totalRows        the total number of rows that will be in the table
     * @param  generator        a method to find the nth row in sort order
     * @param  schema           the schema
     * @return                  the partition tree
     */
    public static PartitionTree createPartitionTreeWithRowsPerPartitionAndTotal(int rowsPerPartition, long totalRows, NthRowGenerator generator, Schema schema) {
        List<Object> splitPoints = new ArrayList<>();
        Field splitField = schema.getRowKeyFields().get(0);
        for (long i = rowsPerPartition; i < totalRows; i += rowsPerPartition) {
            splitPoints.add(generator.getNthRow(i).get(splitField.getName()));
        }
        return PartitionsFromSplitPoints.treeFrom(schema, splitPoints);
    }

    /**
     * A generator for the nth row in sort order to generate test data on the fly. Used when the test data is defined
     * deterministically to avoid needing to hold it all in memory at once.
     */
    @FunctionalInterface
    public interface NthRowGenerator {

        /**
         * Gets the nth row in the test data in sort order.
         *
         * @param  n the index of the row, starting from 0
         * @return   the row
         */
        Row getNthRow(long n);
    }
}
