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
package sleeper.bulkimport.job.runner;

import com.facebook.collections.ByteArray;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.collection.JavaConverters;
import sleeper.bulkimport.job.runner.dataframelocalsort.PartitionAsIntColumn;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionAsIntColumnTest {

    @Test
    public void shouldComputeColumnIfOneSplitIntKey() {
        // Given
        Field rowKeyField = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(rowKeyField).build();
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(10)).construct();
        for (Partition p : partitions) {
            if (p.isLeafPartition() && Integer.MIN_VALUE == (int) p.getRegion().getRange("key").getMin()) {
                p.setId("A");
            } else if (p.isLeafPartition()) {
                p.setId("B");
            } else {
                p.setChildPartitionIds(Arrays.asList("A", "B"));
            }
        }
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When
        Column column = PartitionAsIntColumn.getColumn(partitionTree, schema);

        // Then
        SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(Integer.MIN_VALUE));
        list.add(RowFactory.create(1));
        list.add(RowFactory.create(10));
        list.add(RowFactory.create(100));
        Dataset<Row> df = spark.createDataFrame(list, new StructTypeFactory().getStructType(schema))
                .withColumn("partitionAsInt", column);
        Set<Row> rowsWithPartitionIds = new HashSet<>(df.collectAsList());
        Set<Row> expected = new HashSet<>();
        expected.add(RowFactory.create(Integer.MIN_VALUE, 0));
        expected.add(RowFactory.create(1, 0));
        expected.add(RowFactory.create(10, 1));
        expected.add(RowFactory.create(100, 1));
        assertThat(rowsWithPartitionIds).isEqualTo(expected);
    }

    @Test
    public void shouldComputeColumnIfOneSplitStringKey() {
        // Given
        Field rowKeyField = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(rowKeyField).build();
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList("E")).construct();
        for (Partition p : partitions) {
            if (p.isLeafPartition() && "".equals(p.getRegion().getRange("key").getMin())) {
                p.setId("A");
            } else if (p.isLeafPartition()) {
                p.setId("B");
            } else {
                p.setChildPartitionIds(Arrays.asList("A", "B"));
            }
        }
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When
        Column column = PartitionAsIntColumn.getColumn(partitionTree, schema);

        // Then
        SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(""));
        list.add(RowFactory.create("A"));
        list.add(RowFactory.create("E"));
        list.add(RowFactory.create("ZZZ"));
        Dataset<Row> df = spark.createDataFrame(list, new StructTypeFactory().getStructType(schema))
                .withColumn("partitionAsInt", column);
        Set<Row> rowsWithPartitionIds = new HashSet<>(df.collectAsList());
        Set<Row> expected = new HashSet<>();
        expected.add(RowFactory.create("", 0));
        expected.add(RowFactory.create("A", 0));
        expected.add(RowFactory.create("E", 1));
        expected.add(RowFactory.create("ZZZ", 1));
        assertThat(rowsWithPartitionIds).isEqualTo(expected);
    }

    @Test
    public void shouldComputeColumnIfOneSplitByteArrayKey() {
        // Given
        Field rowKeyField = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(rowKeyField).build();
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(new byte[]{25}))
                .construct();
        for (Partition p : partitions) {
            if (p.isLeafPartition() && Arrays.equals(new byte[]{}, (byte[]) p.getRegion().getRange("key").getMin())) {
                p.setId("A");
            } else if (p.isLeafPartition()) {
                p.setId("B");
            } else {
                p.setChildPartitionIds(Arrays.asList("A", "B"));
            }
        }
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When
        Column column = PartitionAsIntColumn.getColumn(partitionTree, schema);

        // Then
        List<Row> keyToExpectedPartitionId = new ArrayList<>();
        keyToExpectedPartitionId.add(RowFactory.create(new byte[]{}));
        keyToExpectedPartitionId.add(RowFactory.create(new byte[]{1}));
        keyToExpectedPartitionId.add(RowFactory.create(new byte[]{25}));
        keyToExpectedPartitionId.add(RowFactory.create(new byte[]{25, 0}));
        keyToExpectedPartitionId.add(RowFactory.create(new byte[]{64, 1}));
        validatePartitions(keyToExpectedPartitionId, Arrays.asList(0, 0, 1, 1, 1), schema, column);
    }

    private void validatePartitions(List<Row> keys, List<Integer> expectedPartitionIds, Schema schema, Column column) {
        SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
        Dataset<Row> df = spark.createDataFrame(keys, new StructTypeFactory().getStructType(schema))
                .withColumn("partitionAsInt", column);
        List<Row> keysWithComputedPartitions = df.collectAsList();
        List<Row> keysWithExpectedPartitions = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            List<Object> objs = new ArrayList<>(JavaConverters.seqAsJavaList(keys.get(i).toSeq()));
            objs.add(expectedPartitionIds.get(i));
            Row row = Row.fromSeq(JavaConverters.asScalaIteratorConverter(objs.iterator()).asScala().toSeq());
            keysWithExpectedPartitions.add(row);
        }
        Set<Row> keysWithExpectedPartitionsSet = new HashSet<>(replaceByteArrays(keysWithExpectedPartitions));
        Set<Row> keysWithComputedPartitionsSet = new HashSet<>(replaceByteArrays(keysWithComputedPartitions));
        assertThat(keysWithExpectedPartitionsSet).isEqualTo(new HashSet<>(keysWithComputedPartitionsSet));
        spark.stop();
    }

    private static List<Row> replaceByteArrays(List<Row> rows) {
        List<Row> replaced = new ArrayList<>();
        for (Row row : rows) {
            List<Object> objs = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                Object obj = row.get(i);
                if (obj instanceof byte[]) {
                    objs.add(ByteArray.wrap((byte[]) obj));
                } else {
                    objs.add(obj);
                }
            }
            replaced.add(RowFactory.create(objs));
        }
        return replaced;
    }
}
