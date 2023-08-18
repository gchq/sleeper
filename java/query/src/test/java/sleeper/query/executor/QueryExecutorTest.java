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
package sleeper.query.executor;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.iterator.impl.SecurityFilteringIterator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestFactory;
import sleeper.query.QueryException;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.statestore.FixedStateStoreProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithPartitions;

public class QueryExecutorTest {
    protected static ExecutorService executorService;

    @TempDir
    public Path folder;

    @BeforeAll
    public static void initExecutorService() {
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterAll
    public static void shutdownExecutorService() {
        executorService.shutdown();
    }

    @Test
    public void shouldReturnNothingWhenThereAreNoFiles() throws Exception {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).isExhausted();
        }

        // When 2
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 3
        assertThat(leafPartitionQueries).isEmpty();
    }

    @Test
    public void shouldReturnCorrectDataWhenOneRecordInOneFileInOnePartition() throws Exception {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        ingestData(instanceProperties, stateStore, tableProperties, getRecords().iterator());
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable()
                    .containsExactly(getRecords().get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange(field, -10L, true, 1L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).toIterable()
                    .containsExactly(getRecords().get(0));
        }

        // When 4
        region = new Region(rangeFactory.createRange(field, 10L, true, 100L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {
            // Then 4
            assertThat(results).isExhausted();
        }

        // When 5
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 5
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenMultipleIdenticalRecordsInOneFileInOnePartition() throws Exception {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        ingestData(instanceProperties, stateStore, tableProperties, getMultipleIdenticalRecords().iterator());
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getRecords().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getRecords().get(0)));
        }

        // When 4
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 4
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenIdenticalRecordsInMultipleFilesInOnePartition()
            throws StateStoreException, IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, tableProperties, getRecords().iterator());
        }
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getRecords().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getRecords().get(0)));
        }

        // When 4
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 4
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenRecordsInMultipleFilesInOnePartition()
            throws StateStoreException, IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, tableProperties, getMultipleRecords().iterator());
        }
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getMultipleRecords().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 5L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getMultipleRecords().get(4)));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).isExhausted();
        }

        // When 4
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 4
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRecords());
        }

        // When 5
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 5
            assertThat(results).toIterable().hasSize(90)
                    .hasSameElementsAs(getMultipleRecords().stream()
                            .filter(r -> ((long) r.get("key")) >= 1L && ((long) r.get("key")) < 10L)
                            .collect(Collectors.toList()));
        }

        // When 6
        region = new Region(rangeFactory.createRange(field, 1L, false, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 6
            assertThat(results).toIterable().hasSize(80)
                    .hasSameElementsAs(getMultipleRecords().stream()
                            .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) < 10L)
                            .collect(Collectors.toList()));
        }

        // When 7
        region = new Region(rangeFactory.createRange(field, 1L, false, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 7
            assertThat(results).toIterable().hasSize(90)
                    .hasSameElementsAs(getMultipleRecords().stream()
                            .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) <= 10L)
                            .collect(Collectors.toList()));
        }

        // When 8
        region = new Region(rangeFactory.createRange(field, -100000L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 8
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRecords());
        }

        // When 9
        region = new Region(rangeFactory.createRange(field, 5L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 9
            assertThat(results).toIterable().hasSize(60)
                    .hasSameElementsAs(getMultipleRecords().stream()
                            .filter(r -> ((long) r.get("key")) >= 5L)
                            .collect(Collectors.toList()));
        }

        // When 10
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 10
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenRecordsInMultipleFilesInMultiplePartitions()
            throws StateStoreException, IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildTree();
        StateStore stateStore = inMemoryStateStoreWithPartitions(
                tree.getAllPartitions()
        );
        Partition leftPartition = tree.getPartition("left");
        Partition rightPartition = tree.getPartition("right");

        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, tableProperties, getMultipleRecords().iterator());
        }
        List<String> filesInLeftPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(leftPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInRightPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(rightPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getMultipleRecords().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 5L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getMultipleRecords().get(4)));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results.hasNext()).isFalse();
        }

        // When 4
        region = new Region(rangeFactory.createRange(field, -100000L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 4
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRecords());
        }

        // When 5
        region = new Region(rangeFactory.createRange(field, 5L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 5
            assertThat(results).toIterable().hasSize(60)
                    .hasSameElementsAs(getMultipleRecords()
                            .stream().filter(r -> ((long) r.get("key")) >= 5L)
                            .collect(Collectors.toList()));
        }

        // When 6
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 6
        assertThat(leafPartitionQueries).hasSize(2);
        LeafPartitionQuery leftLeafPartitionQuery;
        LeafPartitionQuery rightLeafPartitionQuery;
        if (leafPartitionQueries.get(0).getLeafPartitionId().equals(leftPartition.getId())) {
            leftLeafPartitionQuery = leafPartitionQueries.get(0);
            rightLeafPartitionQuery = leafPartitionQueries.get(1);
        } else {
            leftLeafPartitionQuery = leafPartitionQueries.get(1);
            rightLeafPartitionQuery = leafPartitionQueries.get(0);
        }
        LeafPartitionQuery expectedLeftLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leftLeafPartitionQuery.getSubQueryId(), region, leftPartition.getId(), leftPartition.getRegion(), filesInLeftPartition)
                .build();
        assertThat(leftLeafPartitionQuery).isEqualTo(expectedLeftLeafPartitionQuery);
        LeafPartitionQuery expectedRightLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", rightLeafPartitionQuery.getSubQueryId(), region, rightPartition.getId(), rightPartition.getRegion(), filesInRightPartition)
                .build();
        assertThat(rightLeafPartitionQuery).isEqualTo(expectedRightLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWithMultidimRowKey()
            throws StateStoreException, IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = Schema.builder()
                .rowKeyFields(field1, field2)
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(
                new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "left", "right", 5L)
                        .buildList()
        );
        Partition leftPartition = stateStore.getLeafPartitions().stream()
                .filter(p -> ((long) p.getRegion().getRange("key1").getMin() == Long.MIN_VALUE))
                .findFirst()
                .get();
        Partition rightPartition = stateStore.getLeafPartitions().stream()
                .filter(p -> ((long) p.getRegion().getRange("key1").getMin() == 5L))
                .findFirst()
                .get();
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, tableProperties, getMultipleRecordsMultidimRowKey().iterator());
        }
        List<String> filesInLeftPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(leftPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInRightPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(rightPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Range range1 = rangeFactory.createExactRange(field1, 1L);
        Range range2 = rangeFactory.createExactRange(field2, "1");
        Region region = new Region(Arrays.asList(range1, range2));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getMultipleRecordsMultidimRowKey().get(0)));
        }

        // When 2
        range1 = rangeFactory.createExactRange(field1, 5L);
        range2 = rangeFactory.createExactRange(field2, "5");
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(record -> assertThat(record).isEqualTo(getMultipleRecordsMultidimRowKey().get(4)));
        }

        // When 3
        range1 = rangeFactory.createExactRange(field1, 8L);
        range2 = rangeFactory.createExactRange(field2, "notthere");
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).isExhausted();
        }

        // When 4
        range1 = rangeFactory.createRange(field1, -100000L, true, 123456789L, true);
        range2 = rangeFactory.createRange(field2, "0", true, "99999999999", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 4
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRecordsMultidimRowKey());
        }

        // When 5
        range1 = rangeFactory.createRange(field1, 2L, true, 5L, true);
        range2 = rangeFactory.createRange(field2, "3", true, "6", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 5
            assertThat(results).toIterable().hasSize(30)
                    .hasSameElementsAs(getMultipleRecordsMultidimRowKey().stream()
                            .filter(r -> ((long) r.get("key1")) >= 2L && ((long) r.get("key1")) <= 5L)
                            .filter(r -> ((String) r.get("key2")).compareTo("3") >= 0 && ((String) r.get("key2")).compareTo("6") <= 0)
                            .collect(Collectors.toList()));
        }

        // When 6
        range1 = rangeFactory.createRange(field1, 2L, true, 500L, true);
        range2 = rangeFactory.createRange(field2, "3", true, "6", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 6
        assertThat(leafPartitionQueries).hasSize(2);
        LeafPartitionQuery leftLeafPartitionQuery;
        LeafPartitionQuery rightLeafPartitionQuery;
        if (leafPartitionQueries.get(0).getLeafPartitionId().equals(leftPartition.getId())) {
            leftLeafPartitionQuery = leafPartitionQueries.get(0);
            rightLeafPartitionQuery = leafPartitionQueries.get(1);
        } else {
            leftLeafPartitionQuery = leafPartitionQueries.get(1);
            rightLeafPartitionQuery = leafPartitionQueries.get(0);
        }
        LeafPartitionQuery expectedLeftLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leftLeafPartitionQuery.getSubQueryId(), region, leftPartition.getId(), leftPartition.getRegion(), filesInLeftPartition)
                .build();
        assertThat(leftLeafPartitionQuery).isEqualTo(expectedLeftLeafPartitionQuery);
        LeafPartitionQuery expectedRightLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", rightLeafPartitionQuery.getSubQueryId(), region, rightPartition.getId(), rightPartition.getRegion(), filesInRightPartition)
                .build();
        assertThat(rightLeafPartitionQuery).isEqualTo(expectedRightLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenRecordsInMultipleFilesInMultiplePartitionsMultidimensionalKey()
            throws StateStoreException, QueryException,
            IOException, IteratorException, ObjectFactoryException {
        // Given
        Field field1 = new Field("key1", new StringType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = Schema.builder()
                .rowKeyFields(field1, field2)
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        //  Partitions:
        //  - Root partition covers the whole space
        //  - Root has 2 children: one is 1 and 3 below, the other is 2 and 4
        //  - There are 4 leaf partitions:
        //      null +-----------+-----------+
        //           |     3     |    4      |
        //           |           |           |
        //       "T" +-----------+-----------+
        //           |           |           |
        //           |     1     |    2      |
        //           |           |           |
        //           |           |           |
        //        "" +-----------+-----------+
        //           ""         "I"          null      (Dimension 1)
        // Add 4 records - record i is in the center of partition i
        Record record1 = createRecordMultidimensionalKey("D", "J", 10L, 100L);
        Record record2 = createRecordMultidimensionalKey("K", "H", 1000L, 10000L);
        Record record3 = createRecordMultidimensionalKey("C", "X", 100000L, 1000000L);
        Record record4 = createRecordMultidimensionalKey("P", "Z", 10000000L, 100000000L);
        List<Record> records = Arrays.asList(record1, record2, record3, record4);

        StateStore stateStore = inMemoryStateStoreWithPartitions(
                new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .buildList());

        ingestData(instanceProperties, stateStore, tableProperties, records.iterator());

        // Split the root partition into 2:
        stateStore.initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "I")
                .buildList()
        );


        ingestData(instanceProperties, stateStore, tableProperties, records.iterator());

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "I")
                .splitToNewChildrenOnDimension("left", "P1", "P3", 1, "T")
                .splitToNewChildrenOnDimension("right", "P2", "P4", 1, "T")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        ingestData(instanceProperties, stateStore, tableProperties, records.iterator());

        List<String> filesInLeafPartition1 = stateStore.getActiveFiles().stream()
                .filter(f -> List.of("P1", "left", "root").contains(f.getPartitionId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition2 = stateStore.getActiveFiles().stream()
                .filter(f -> List.of("P2", "right", "root").contains(f.getPartitionId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition3 = stateStore.getActiveFiles().stream()
                .filter(f -> List.of("P3", "left", "root").contains(f.getPartitionId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition4 = stateStore.getActiveFiles().stream()
                .filter(f -> List.of("P4", "right", "root").contains(f.getPartitionId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());

        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1 - query for entire space
        Range range1 = rangeFactory.createRange(field1, "", true, null, false);
        Range range2 = rangeFactory.createRange(field2, "", true, null, false);
        Region region = new Region(Arrays.asList(range1, range2));

        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable().hasSize(12) // 12 because the same data was added 3 times at different levels of the tree
                    .hasSameElementsAs(records);
        }

        // When 2 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", true, "H", true);
        range2 = rangeFactory.createRange(field2, "", true, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(record1));
        }

        // When 3 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", true, "H", false);
        range2 = rangeFactory.createRange(field2, "", true, "S", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(record1));
        }

        // When 4 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", false, "H", true);
        range2 = rangeFactory.createRange(field2, "", false, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 4
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(record1));
        }

        // When 5 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", false, "H", false);
        range2 = rangeFactory.createRange(field2, "", false, "S", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 5
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(record1));
        }

        // When 6 - query for range within partitions 1 and 2
        range1 = rangeFactory.createRange(field1, "", true, "Z", true);
        range2 = rangeFactory.createRange(field2, "", true, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 6
            assertThat(results).toIterable().hasSize(6)
                    .hasSameElementsAs(Arrays.asList(record1, record2));
        }

        // When 7 - query for range to the right of the data in partitions 2 and 4
        range1 = rangeFactory.createRange(field1, "T", true, "Z", true);
        range2 = rangeFactory.createRange(field2, "", true, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 7
            assertThat(results).isExhausted();
        }

        // When 8 - query for a 1-dimensional range
        range1 = rangeFactory.createRange(field1, "J", true, "Z", true);
        region = new Region(range1);
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 8
            assertThat(results).toIterable().hasSize(6)
                    .hasSameElementsAs(Arrays.asList(record2, record4));
        }

        // When 9 - query for a range where the first dimension is constant
        range1 = rangeFactory.createExactRange(field1, "C");
        range2 = rangeFactory.createRange(field2, "", true, null, true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 9
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(record3));
        }

        // When 10 - query for a range where the max equals record1 and max is not inclusive
        range1 = rangeFactory.createRange(field1, "", true, "D", false);
        range2 = rangeFactory.createRange(field2, "", true, "T", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 10
            assertThat(results).isExhausted();
        }

        // When 11 - query for a range where the max equals record1 and max is inclusive
        range1 = rangeFactory.createRange(field1, "", true, "D", true);
        range2 = rangeFactory.createRange(field2, "", true, "T", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 11
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(record1));
        }

        // When 12 - query for a range where the boundaries cover all 4 records, min is inclusive, max is not inclusive
        // Record i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - no
        range1 = rangeFactory.createRange(field1, "C", true, "P", false);
        range2 = rangeFactory.createRange(field2, "H", true, "Z", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 12
            assertThat(results).toIterable().hasSize(9)
                    .hasSameElementsAs(Arrays.asList(record1, record2, record3));
        }

        // When 13 - query for a range where the boundaries cover all 4 records, min is inclusive, and max is inclusive
        // Record i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - yes
        range1 = rangeFactory.createRange(field1, "C", true, "P", true);
        range2 = rangeFactory.createRange(field2, "H", true, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 13
            assertThat(results).toIterable().hasSize(12)
                    .hasSameElementsAs(Arrays.asList(record1, record2, record3, record4));
        }

        // When 14 - query for a range where the boundaries cover all 4 records, min is not inclusive, and max is not inclusive
        // Record i is in range? 1 - yes; 2 - no; 3 - no; 4 - no
        range1 = rangeFactory.createRange(field1, "C", false, "P", false);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 14
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(record1));
        }

        // When 15 - query for a range where the boundaries cover all 4 records, min is not inclusive, and max is inclusive
        // Record i is in range? 1 - yes; 2 - no; 3 - no; 4 - yes
        range1 = rangeFactory.createRange(field1, "C", false, "P", true);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 15
            assertThat(results).toIterable().hasSize(6)
                    .hasSameElementsAs(Arrays.asList(record1, record4));
        }

        // When 16
        range1 = rangeFactory.createRange(field1, "C", false, "P", true);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 16
        assertThat(leafPartitionQueries).hasSize(4);
        LeafPartitionQuery leafPartition1Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P1"))
                .findFirst()
                .get();
        LeafPartitionQuery leafPartition2Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P2"))
                .findFirst()
                .get();
        LeafPartitionQuery leafPartition3Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P3"))
                .findFirst()
                .get();
        LeafPartitionQuery leafPartition4Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P4"))
                .findFirst()
                .get();
        LeafPartitionQuery expectedLeafPartition1Query = new LeafPartitionQuery.Builder(
                "myTable", "id", leafPartition1Query.getSubQueryId(), region, "P1",
                tree.getPartition("P1").getRegion(), filesInLeafPartition1
        ).build();
        assertThat(leafPartition1Query).isEqualTo(expectedLeafPartition1Query);
        LeafPartitionQuery expectedLeafPartition2Query = new LeafPartitionQuery.Builder(
                "myTable", "id", leafPartition2Query.getSubQueryId(), region, "P2",
                tree.getPartition("P2").getRegion(), filesInLeafPartition2
        ).build();
        assertThat(leafPartition2Query).isEqualTo(expectedLeafPartition2Query);
        LeafPartitionQuery expectedLeafPartition3Query = new LeafPartitionQuery.Builder(
                "myTable", "id", leafPartition3Query.getSubQueryId(), region, "P3",
                tree.getPartition("P3").getRegion(), filesInLeafPartition3
        ).build();
        assertThat(leafPartition3Query).isEqualTo(expectedLeafPartition3Query);
        LeafPartitionQuery expectedLeafPartition4Query = new LeafPartitionQuery.Builder(
                "myTable", "id", leafPartition4Query.getSubQueryId(), region, "P4",
                tree.getPartition("P4").getRegion(), filesInLeafPartition4
        ).build();
        assertThat(leafPartition4Query).isEqualTo(expectedLeafPartition4Query);
    }

    @Test
    public void shouldReturnDataCorrectlySorted()
            throws StateStoreException, IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder()
                .rowKeyFields(field)
                .sortKeyFields(new Field("value1", new LongType()))
                .valueFields(new Field("value2", new LongType()))
                .build();
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(
                new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "left", "right", 5L)
                        .buildList()
        );
        ingestData(instanceProperties, stateStore, tableProperties, getMultipleRecordsForTestingSorting().iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable()
                    .containsExactlyElementsOf(getMultipleRecordsForTestingSorting()
                            .stream()
                            .filter(r -> ((long) r.get("key")) == 1L)
                            .sorted(Comparator.comparing(r -> ((Long) r.get("value1"))))
                            .collect(Collectors.toList()));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 5L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).toIterable()
                    .containsExactlyElementsOf(getMultipleRecordsForTestingSorting()
                            .stream()
                            .filter(r -> ((long) r.get("key")) == 5L)
                            .sorted(Comparator.comparing(r -> ((Long) r.get("value1"))))
                            .collect(Collectors.toList()));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).isExhausted();
        }
    }

    @Test
    public void shouldReturnCorrectDataWhenOneRecordInOneFileInOnePartitionAndCompactionIteratorApplied()
            throws StateStoreException, IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Field field = new Field("id", new StringType());
        Schema schema = Schema.builder()
                .rowKeyFields(field)
                .valueFields(new Field("timestamp", new LongType()))
                .build();
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        List<Record> records = getRecordsForAgeOffIteratorTest();
        ingestData(instanceProperties, stateStore, tableProperties, records.iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, "1"));
        Query query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 1
            assertThat(results).toIterable().containsExactly(records.get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, "0"));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, "2"));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 3
            assertThat(results).isExhausted();
        }

        // When 4
        region = new Region(rangeFactory.createExactRange(field, "3"));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 4
            assertThat(results).isExhausted();
        }

        // When 5
        region = new Region(rangeFactory.createExactRange(field, "4"));
        query = new Query.Builder("myTable", "id", region).build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then 5
            assertThat(results).toIterable().containsExactly(records.get(3));
        }
    }

    @Test
    public void shouldReturnCorrectDataWhenQueryTimeIteratorApplied()
            throws IteratorException, IOException, StateStoreException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getSecurityLabelSchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = inMemoryStateStoreWithPartitions(
                new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "left", "right", 5L)
                        .buildList()
        );
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, tableProperties,
                    getRecordsForQueryTimeIteratorTest(i % 2 == 0 ? "notsecret" : "secret").iterator());
        }
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region)
                .setQueryTimeIteratorClassName(SecurityFilteringIterator.class.getName())
                .setQueryTimeIteratorConfig("securityLabel,notsecret")
                .build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then
            Record expected = getRecordsForQueryTimeIteratorTest("notsecret").get(0);
            assertThat(results).toIterable().hasSize(5)
                    .allSatisfy(result -> assertThat(result).isEqualTo(expected));
        }
    }

    @Test
    public void shouldReturnOnlyRequestedValuesWhenSpecified()
            throws StateStoreException, IteratorException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        ingestData(instanceProperties, stateStore, tableProperties, getRecords().iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, "/tmp"), tableProperties, stateStore,
                new Configuration(), Executors.newFixedThreadPool(1));
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("unused", "abc", region)
                .setQueryTimeIteratorClassName(null)
                .setQueryTimeIteratorConfig(null)
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(Lists.newArrayList("value2"))
                .build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then
            assertThat(results).toIterable().hasSize(1)
                    .flatExtracting(Record::getKeys).doesNotContain("value1").contains("key", "value2");
        }
    }

    @Test
    public void shouldIncludeFieldsRequiredByIteratorsEvenIfNotSpecifiedByTheUser()
            throws StateStoreException, IteratorException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getSecurityLabelSchema();
        Field field = schema.getRowKeyFields().get(0);
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        ingestData(instanceProperties, stateStore, tableProperties, getRecordsForQueryTimeIteratorTest("secret").iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, "/tmp"), tableProperties, stateStore,
                new Configuration(), Executors.newFixedThreadPool(1));
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("unused", "abc", region)
                .setQueryTimeIteratorClassName(SecurityFilteringIterator.class.getName())
                .setQueryTimeIteratorConfig("securityLabel,secret")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(Lists.newArrayList("value"))
                .build();
        try (CloseableIterator<Record> results = queryExecutor.execute(query)) {

            // Then
            assertThat(results).hasNext().toIterable().allSatisfy(result ->
                    assertThat(result.getKeys()).contains("key", "value", "securityLabel"));
        }
    }

    protected Schema getLongKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    protected Schema getSecurityLabelSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value", new LongType()), new Field("securityLabel", new StringType()))
                .build();
    }

    protected void ingestData(InstanceProperties instanceProperties, StateStore stateStore,
                              TableProperties tableProperties, Iterator<Record> recordIterator) throws IOException, StateStoreException, IteratorException {
        instanceProperties.set(FILE_SYSTEM, "file://");
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        tableProperties.set(DATA_BUCKET, createTempDirectory(folder, null).toString());
        IngestFactory factory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(folder, null).toString())
                .instanceProperties(instanceProperties)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .hadoopConfiguration(new Configuration())
                .build();
        factory.ingestFromRecordIterator(tableProperties, recordIterator);
    }

    protected List<Record> getRecords() {
        List<Record> records = new ArrayList<>();
        Record record = new Record();
        record.put("key", 1L);
        record.put("value1", 10L);
        record.put("value2", 100L);
        records.add(record);
        return records;
    }

    protected List<Record> getMultipleIdenticalRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.addAll(getRecords());
        }
        return records;
    }

    protected List<Record> getMultipleRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Record record = new Record();
            record.put("key", (long) i);
            record.put("value1", i * 10L);
            record.put("value2", i * 100L);
            records.add(record);
        }
        return records;
    }

    protected List<Record> getMultipleRecordsMultidimRowKey() {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Record record = new Record();
            record.put("key1", (long) i);
            record.put("key2", String.valueOf(i));
            record.put("value1", i * 10L);
            record.put("value2", i * 100L);
            records.add(record);
        }
        return records;
    }

    protected List<Record> getMultipleRecordsForTestingSorting() {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            for (int j = 1000; j >= 900; j--) {
                Record record = new Record();
                record.put("key", (long) i);
                record.put("value1", (long) j);
                record.put("value2", i * 100L);
                records.add(record);
            }
        }
        return records;
    }

    protected List<Record> getRecordsForAgeOffIteratorTest() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("id", "1");
        record1.put("timestamp", System.currentTimeMillis());
        records.add(record1);
        Record record2 = new Record();
        record2.put("id", "2");
        record2.put("timestamp", System.currentTimeMillis() - 1_000_000_000L);
        records.add(record2);
        Record record3 = new Record();
        record3.put("id", "3");
        record3.put("timestamp", System.currentTimeMillis() - 2_000_000L);
        records.add(record3);
        Record record4 = new Record();
        record4.put("id", "4");
        record4.put("timestamp", System.currentTimeMillis());
        records.add(record4);
        return records;
    }

    protected List<Record> getRecordsForQueryTimeIteratorTest(String securityLabel) {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Record record = new Record();
            record.put("key", (long) i);
            record.put("value", i * 10L);
            record.put("securityLabel", securityLabel);
            records.add(record);
        }
        return records;
    }

    private static Record createRecordMultidimensionalKey(String key1, String key2, long value1, long value2) {
        Record record = new Record();
        record.put("key1", key1);
        record.put("key2", key2);
        record.put("value1", value1);
        record.put("value2", value2);
        return record;
    }

    private static InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        return instanceProperties;
    }
}
