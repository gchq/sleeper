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
package sleeper.bulkimport.job.runner.dataframelocalsort;

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
