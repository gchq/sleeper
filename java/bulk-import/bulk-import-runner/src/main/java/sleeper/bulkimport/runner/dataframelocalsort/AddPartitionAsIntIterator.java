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
package sleeper.bulkimport.runner.dataframelocalsort;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import sleeper.bulkimport.runner.common.PartitionNumbers;
import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Adds an integer ID to each row iterated over, identifying which Sleeper partition it belongs to. Each Sleeper
 * partitition ID is mapped to a different integer.
 * <p>
 * The integer ID is determined by taking the leaf partitions, sorting them by their ID,
 * and then assigning integers 0,1,...,numLeafPartitions -1 to them. This guarantees
 * that two rows from the same leaf partition processed in different tasks/executors
 * will be get the same integer id.
 */
public class AddPartitionAsIntIterator implements Iterator<Row> {
    private final Iterator<Row> input;
    private final Schema schema;
    private final PartitionTree partitionTree;
    private final Map<String, Integer> partitionIdToInt;

    public AddPartitionAsIntIterator(Iterator<Row> input, Schema schema, PartitionTree partitionTree) {
        this.input = input;
        this.schema = schema;
        this.partitionTree = partitionTree;
        this.partitionIdToInt = PartitionNumbers.getPartitionIdToInt(partitionTree);
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Row next() {
        Row row = input.next();
        int numRowKeyFields = schema.getRowKeyFieldNames().size();
        int numFields = schema.getAllFieldNames().size();
        Object[] rowWithPartition = new Object[numFields + 1];
        List<Object> key = new ArrayList<>(numRowKeyFields);
        for (int i = 0; i < numFields; i++) {
            rowWithPartition[i] = row.get(i);
            if (i < numRowKeyFields) {
                key.add(rowWithPartition[i]);
            }
        }

        String partitionId = partitionTree.getLeafPartition(schema, Key.create(key)).getId();
        rowWithPartition[numFields] = partitionIdToInt.get(partitionId);

        return RowFactory.create(rowWithPartition);
    }
}
