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
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionAsIntColumnTest {
    private static SparkSession spark;

    @BeforeClass
    public static void initSpark() {
        spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    }

    @AfterClass
    public static void stopSpark() {
        spark.stop();
    }

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
        List<Row> keys = new ArrayList<>();
        keys.add(RowFactory.create(Integer.MIN_VALUE));
        keys.add(RowFactory.create(1));
        keys.add(RowFactory.create(10));
        keys.add(RowFactory.create(100));
        validatePartitions(keys, Arrays.asList(0, 0, 1, 1), schema, column);
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
        List<Row> keys = new ArrayList<>();
        keys.add(RowFactory.create(""));
        keys.add(RowFactory.create("A"));
        keys.add(RowFactory.create("E"));
        keys.add(RowFactory.create("ZZZ"));
        validatePartitions(keys, Arrays.asList(0, 0, 1, 1), schema, column);
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
        List<Row> keys = new ArrayList<>();
        keys.add(RowFactory.create(new byte[]{}));
        keys.add(RowFactory.create(new byte[]{1}));
        keys.add(RowFactory.create(new byte[]{25}));
        keys.add(RowFactory.create(new byte[]{25, 0}));
        keys.add(RowFactory.create(new byte[]{64, 1}));
        validatePartitions(keys, Arrays.asList(0, 0, 1, 1, 1), schema, column);
    }

    @Test
    public void shouldComputeColumnUnbalancedTree() {
        // Given
        Field rowKeyField = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(rowKeyField).build();
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList("E", "P")).construct();
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When
        Column column = PartitionAsIntColumn.getColumn(partitionTree, schema);

        // Then
        SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(""));
        list.add(RowFactory.create("E"));
        list.add(RowFactory.create("P"));
        list.add(RowFactory.create("ZZZ"));
        Dataset<Row> df = spark.createDataFrame(list, new StructTypeFactory().getStructType(schema))
                .withColumn("partitionAsInt", column)
                .select("partitionAsInt");
        List<Integer> partitionsAsInt = df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
        Set<Integer> partitionIds = new HashSet<>(partitionsAsInt);
        assertThat(partitionIds).hasSize(3);
        Set<Integer> expectedIds = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            expectedIds.add(i);
        }
        assertThat(partitionIds).isEqualTo(expectedIds);
    }

    @Test
    public void shouldComputeColumnLotsOfLeafPartitions() {
        // Given
        Field rowKeyField = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(rowKeyField).build();
        List<Object> splitPoints = new ArrayList<>();
        // - Split points of 10, 20, ..., 10230
        //   gives 1024 leaf partitions
        for (int i = 1; i < 1024; i++) {
            splitPoints.add(i * 10);
        }
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, splitPoints).construct();
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When
        Column column = PartitionAsIntColumn.getColumn(partitionTree, schema);
        System.out.println(column);

        // Then
        SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
        List<Row> list = new ArrayList<>();
        // - Create points in each of the 1024 partitions
        for (int i = 0; i < 1024; i++) {
            list.add(RowFactory.create(i * 10));
        }
        Dataset<Row> df = spark.createDataFrame(list, new StructTypeFactory().getStructType(schema))
                .withColumn("partitionAsInt", column)
                .select("partitionAsInt");
        List<Integer> partitionsAsInt = df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
        Set<Integer> partitionIds = new HashSet<>(partitionsAsInt);
        assertThat(partitionIds).hasSize(1024);
        Set<Integer> expectedIds = new HashSet<>();
        for (int i = 0; i < 1024; i++) {
            expectedIds.add(i);
        }
        assertThat(partitionIds).isEqualTo(expectedIds);
    }

    private void validatePartitions(List<Row> keys, List<Integer> expectedPartitionIds, Schema schema, Column column) {
        StructType structType = new StructTypeFactory().getStructType(schema);
        for (int i = 0; i < keys.size(); i++) {
            Dataset<Row> df = spark
                .createDataFrame(Collections.singletonList(keys.get(i)), structType)
                .withColumn("partitionAsInt", column);
            Row keyWithComputedPartition = df.collectAsList().get(0);
            List<Object> objs = new ArrayList<>(JavaConverters.seqAsJavaList(keys.get(i).toSeq()));
            objs.add(expectedPartitionIds.get(i));
            Row expectedRow = Row.fromSeq(JavaConverters.asScalaIteratorConverter(objs.iterator()).asScala().toSeq());
            assertThat(replaceByteArrays(expectedRow)).isEqualTo(replaceByteArrays(keyWithComputedPartition));
        }
    }

    private static Row replaceByteArrays(Row row) {
        List<Object> objs = new ArrayList<>();
        for (int i = 0; i < row.size(); i++) {
            Object obj = row.get(i);
            if (obj instanceof byte[]) {
                objs.add(ByteArray.wrap((byte[]) obj));
            } else {
                objs.add(obj);
            }
        }
        return RowFactory.create(objs);
    }
}
