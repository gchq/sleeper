/*
 * Copyright 2022 Crown Copyright
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
package sleeper.bulkimport.job.runner.dataframe.localsort;

import org.apache.spark.sql.Column;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class PartitionAsIntColumn {

    private PartitionAsIntColumn() {
    }

    /**
     * Returns a Column which computes an integer identifying the partition that a record is in.
     * This is done by creating a mapping from the leaf partitions to the integers 0, 1, ..., numLeafPartitions - 1.
     * This mapping is done in a deterministic manner, namely the leaf partitions are sorted by their id and then
     * integers 0, 1, ... are assigned by iterating through the leaf partitions in ascending order. The way in
     * which the mapping from the leaf partitions to integers is computed does not matter, all that matters is that
     * each leaf partition gets mapped to one and only one int, and that no other partition gets mapped to
     * the same int.
     * 
     * As an example, if we have a table with a key field of name 'key' and type integer, and there are two leaf
     * partitions where 10 is the boundary between them then the column will be the expression
     * "CASE WHEN (key < 10) THEN 0 ELSE 1 END".
     * 
     * @param partitionTree The PartitionTree of the table
     * @param schema The Schema of the table
     * @return A Column calculating an int that identifies the partition the record is in
     */
    public static Column getColumn(PartitionTree partitionTree, Schema schema) {
        Map<String, Integer> partitionIdToInt = getPartitionIdToInt(partitionTree);
        Map<Integer, Column> rowKeyDimensionToColumn = getRowKeyDimensionToColumn(schema);
        Partition rootPartition = partitionTree.getRootPartition();
        return computeColumn(partitionTree, rootPartition, partitionIdToInt, rowKeyDimensionToColumn);
    }

    private static Column computeColumn(PartitionTree partitionTree, Partition partition,
            Map<String, Integer> partitionIdToInt, Map<Integer, Column> rowKeyDimensionToColumn) {
        if (partition.isLeafPartition()) {
            return lit(partitionIdToInt.get(partition.getId()));
        } else {
            Partition left = partitionTree.getPartition(partition.getChildPartitionIds().get(0));
            Partition right = partitionTree.getPartition(partition.getChildPartitionIds().get(1));
            Object max = left.getRegion().getRanges().get(partition.getDimension()).getMax();
            Column column = rowKeyDimensionToColumn.get(partition.getDimension());
            return when(column.lt(max), computeColumn(partitionTree, left, partitionIdToInt, rowKeyDimensionToColumn))
                    .otherwise(computeColumn(partitionTree, right, partitionIdToInt, rowKeyDimensionToColumn));
        }
    }

    private static Map<String, Integer> getPartitionIdToInt(PartitionTree partitionTree) {
        SortedSet<String> sortedLeafPartitionIds = new TreeSet<>();
        partitionTree.getAllPartitions().stream().filter(Partition::isLeafPartition).map(Partition::getId)
                .forEach(sortedLeafPartitionIds::add);
        Map<String, Integer> partitionIdToInt = new TreeMap<>();
        int i = 0;
        for (String leafPartitionId : sortedLeafPartitionIds) {
            partitionIdToInt.put(leafPartitionId, i);
            i++;
        }
        return partitionIdToInt;
    }

    private static Map<Integer, Column> getRowKeyDimensionToColumn(Schema schema) {
        Map<Integer, Column> rowKeyDimensionToColumn = new HashMap<>();
        int i = 0;
        for (Field field : schema.getRowKeyFields()) {
            rowKeyDimensionToColumn.put(i, new Column(field.getName()));
            i++;
        }
        return rowKeyDimensionToColumn;
    }
}
