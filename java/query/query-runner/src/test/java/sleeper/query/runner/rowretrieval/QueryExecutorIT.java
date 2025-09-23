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
package sleeper.query.runner.rowretrieval;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.AgeOffIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.example.iterator.SecurityFilteringIterator;
import sleeper.ingest.runner.IngestFactory;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.rowretrieval.QueryExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DATA_ENGINE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class QueryExecutorIT {
    private static ExecutorService executorService;

    @TempDir
    public Path tempDir;

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());

    @BeforeAll
    public static void initExecutorService() {
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterAll
    public static void shutdownExecutorService() {
        executorService.shutdown();
    }

    @BeforeEach
    void setUp() throws IOException {
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, createTempDirectory(tempDir, null).toString());
        instanceProperties.setEnum(DEFAULT_DATA_ENGINE, DataEngine.JAVA);
    }

    @Test
    public void shouldReturnNothingWhenThereAreNoFiles() throws Exception {
        // Given
        tableProperties.setSchema(getLongKeySchema());
        update(stateStore).initialise(tableProperties);
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When 1
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).isExhausted();
        }

        // When 2
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(queryWithRegion(region));

        // Then 3
        assertThat(leafPartitionQueries).isEmpty();
    }

    @Test
    public void shouldReturnCorrectDataWhenOneRowInOneFileInOnePartition() throws Exception {
        // Given
        tableProperties.setSchema(getLongKeySchema());
        update(stateStore).initialise(tableProperties);
        ingestData(getRows().iterator());
        List<String> files = filenamesInPartition("root");
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When 1
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable()
                    .containsExactly(getRows().get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange("key", 0L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange("key", -10L, true, 1L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).toIterable()
                    .containsExactly(getRows().get(0));
        }

        // When 4
        region = new Region(rangeFactory.createRange("key", 10L, true, 100L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {
            // Then 4
            assertThat(results).isExhausted();
        }

        // When 5
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        Query query = queryWithRegion(region);
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 5
        assertThat(leafPartitionQueries)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactly(LeafPartitionQuery.builder()
                        .parentQuery(query)
                        .tableId(tableProperties.get(TABLE_ID))
                        .subQueryId("ignored")
                        .regions(List.of(region))
                        .leafPartitionId("root")
                        .partitionRegion(rootPartitionRegion())
                        .files(files)
                        .build());
    }

    @Test
    public void shouldReturnCorrectDataWhenMultipleIdenticalRowsInOneFileInOnePartition() throws Exception {
        // Given
        tableProperties.setSchema(getLongKeySchema());
        update(stateStore).initialise(tableProperties);
        ingestData(getMultipleIdenticalRows().iterator());
        List<String> files = filenamesInPartition("root");
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When 1
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getRows().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange("key", 0L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getRows().get(0)));
        }

        // When 4
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        Query query = queryWithRegion(region);
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 4
        assertThat(leafPartitionQueries)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactly(LeafPartitionQuery.builder()
                        .parentQuery(query)
                        .tableId(tableProperties.get(TABLE_ID))
                        .subQueryId("ignored")
                        .regions(List.of(region))
                        .leafPartitionId("root")
                        .partitionRegion(rootPartitionRegion())
                        .files(files)
                        .build());
    }

    @Test
    public void shouldReturnCorrectDataWhenIdenticalRowsInMultipleFilesInOnePartition() throws IOException, IteratorCreationException, ObjectFactoryException, QueryException {
        // Given
        tableProperties.setSchema(getLongKeySchema());
        update(stateStore).initialise(tableProperties);
        for (int i = 0; i < 10; i++) {
            ingestData(getRows().iterator());
        }
        List<String> files = filenamesInPartition("root");
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When 1
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getRows().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange("key", 0L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getRows().get(0)));
        }

        // When 4
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        Query query = queryWithRegion(region);
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 4
        assertThat(leafPartitionQueries)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactly(LeafPartitionQuery.builder()
                        .parentQuery(query)
                        .tableId(tableProperties.get(TABLE_ID))
                        .subQueryId("ignored")
                        .regions(List.of(region))
                        .leafPartitionId("root")
                        .partitionRegion(rootPartitionRegion())
                        .files(files)
                        .build());
    }

    @Test
    public void shouldReturnCorrectDataWhenRowsInMultipleFilesInOnePartition() throws IOException, IteratorCreationException, ObjectFactoryException, QueryException {
        // Given
        tableProperties.setSchema(getLongKeySchema());
        update(stateStore).initialise(tableProperties);
        for (int i = 0; i < 10; i++) {
            ingestData(getMultipleRows().iterator());
        }
        List<String> files = filenamesInPartition("root");
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When 1
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange("key", 5L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(4)));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange("key", 0L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).isExhausted();
        }

        // When 4
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 4
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRows());
        }

        // When 5
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 5
            assertThat(results).toIterable().hasSize(90)
                    .hasSameElementsAs(getMultipleRows().stream()
                            .filter(r -> ((long) r.get("key")) >= 1L && ((long) r.get("key")) < 10L)
                            .collect(Collectors.toList()));
        }

        // When 6
        region = new Region(rangeFactory.createRange("key", 1L, false, 10L, false));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 6
            assertThat(results).toIterable().hasSize(80)
                    .hasSameElementsAs(getMultipleRows().stream()
                            .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) < 10L)
                            .collect(Collectors.toList()));
        }

        // When 7
        region = new Region(rangeFactory.createRange("key", 1L, false, 10L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 7
            assertThat(results).toIterable().hasSize(90)
                    .hasSameElementsAs(getMultipleRows().stream()
                            .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) <= 10L)
                            .collect(Collectors.toList()));
        }

        // When 8
        region = new Region(rangeFactory.createRange("key", -100000L, true, 123456789L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 8
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRows());
        }

        // When 9
        region = new Region(rangeFactory.createRange("key", 5L, true, 123456789L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 9
            assertThat(results).toIterable().hasSize(60)
                    .hasSameElementsAs(getMultipleRows().stream()
                            .filter(r -> ((long) r.get("key")) >= 5L)
                            .collect(Collectors.toList()));
        }

        // When 10
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        Query query = queryWithRegion(region);
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 10
        assertThat(leafPartitionQueries)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactly(LeafPartitionQuery.builder()
                        .parentQuery(query)
                        .tableId(tableProperties.get(TABLE_ID))
                        .subQueryId("ignored")
                        .regions(List.of(region))
                        .leafPartitionId("root")
                        .partitionRegion(rootPartitionRegion())
                        .files(files)
                        .build());
    }

    @Test
    public void shouldReturnCorrectDataWhenRowsInMultipleFilesInMultiplePartitions() throws IOException, IteratorCreationException, ObjectFactoryException, QueryException {
        // Given
        tableProperties.setSchema(getLongKeySchema());
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildTree();
        update(stateStore).initialise(tree);

        for (int i = 0; i < 10; i++) {
            ingestData(getMultipleRows().iterator());
        }
        List<String> filesInLeftPartition = filenamesInPartition("left");
        List<String> filesInRightPartition = filenamesInPartition("right");
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When 1
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(0)));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange("key", 5L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(4)));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange("key", 0L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results.hasNext()).isFalse();
        }

        // When 4
        region = new Region(rangeFactory.createRange("key", -100000L, true, 123456789L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 4
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRows());
        }

        // When 5
        region = new Region(rangeFactory.createRange("key", 5L, true, 123456789L, true));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 5
            assertThat(results).toIterable().hasSize(60)
                    .hasSameElementsAs(getMultipleRows()
                            .stream().filter(r -> ((long) r.get("key")) >= 5L)
                            .collect(Collectors.toList()));
        }

        // When 6
        region = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        Query query = queryWithRegion(region);
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 6
        assertThat(leafPartitionQueries)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactlyInAnyOrder(
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("left")
                                .partitionRegion(tree.getPartition("left").getRegion())
                                .files(filesInLeftPartition)
                                .build(),
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("right")
                                .partitionRegion(tree.getPartition("right").getRegion())
                                .files(filesInRightPartition)
                                .build());
    }

    @Test
    public void shouldReturnCorrectDataWithMultidimRowKey() throws IOException, IteratorCreationException, ObjectFactoryException, QueryException {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key1", new LongType()), new Field("key2", new StringType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build());
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildTree();
        update(stateStore).initialise(tree);
        for (int i = 0; i < 10; i++) {
            ingestData(getMultipleRowsMultidimRowKey().iterator());
        }
        List<String> filesInLeftPartition = filenamesInPartition("left");
        List<String> filesInRightPartition = filenamesInPartition("right");
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When 1
        Range range1 = rangeFactory.createExactRange("key1", 1L);
        Range range2 = rangeFactory.createExactRange("key2", "1");
        Region region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRowsMultidimRowKey().get(0)));
        }

        // When 2
        range1 = rangeFactory.createExactRange("key1", 5L);
        range2 = rangeFactory.createExactRange("key2", "5");
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).toIterable().hasSize(10)
                    .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRowsMultidimRowKey().get(4)));
        }

        // When 3
        range1 = rangeFactory.createExactRange("key1", 8L);
        range2 = rangeFactory.createExactRange("key2", "notthere");
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).isExhausted();
        }

        // When 4
        range1 = rangeFactory.createRange("key1", -100000L, true, 123456789L, true);
        range2 = rangeFactory.createRange("key2", "0", true, "99999999999", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 4
            assertThat(results).toIterable().hasSize(100)
                    .hasSameElementsAs(getMultipleRowsMultidimRowKey());
        }

        // When 5
        range1 = rangeFactory.createRange("key1", 2L, true, 5L, true);
        range2 = rangeFactory.createRange("key2", "3", true, "6", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 5
            assertThat(results).toIterable().hasSize(30)
                    .hasSameElementsAs(getMultipleRowsMultidimRowKey().stream()
                            .filter(r -> ((long) r.get("key1")) >= 2L && ((long) r.get("key1")) <= 5L)
                            .filter(r -> ((String) r.get("key2")).compareTo("3") >= 0 && ((String) r.get("key2")).compareTo("6") <= 0)
                            .collect(Collectors.toList()));
        }

        // When 6
        range1 = rangeFactory.createRange("key1", 2L, true, 500L, true);
        range2 = rangeFactory.createRange("key2", "3", true, "6", true);
        region = new Region(Arrays.asList(range1, range2));
        Query query = queryWithRegion(region);
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 6
        assertThat(leafPartitionQueries)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactlyInAnyOrder(
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("left")
                                .partitionRegion(tree.getPartition("left").getRegion())
                                .files(filesInLeftPartition)
                                .build(),
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("right")
                                .partitionRegion(tree.getPartition("right").getRegion())
                                .files(filesInRightPartition)
                                .build());
    }

    @Test
    public void shouldReturnCorrectDataWhenRowsInMultipleFilesInMultiplePartitionsMultidimensionalKey() throws QueryException, IOException, IteratorCreationException, ObjectFactoryException {
        // Given
        Field field1 = new Field("key1", new StringType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = Schema.builder()
                .rowKeyFields(field1, field2)
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
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
        // Add 4 rows - row i is in the center of partition i
        Row row1 = createRowMultidimensionalKey("D", "J", 10L, 100L);
        Row row2 = createRowMultidimensionalKey("K", "H", 1000L, 10000L);
        Row row3 = createRowMultidimensionalKey("C", "X", 100000L, 1000000L);
        Row row4 = createRowMultidimensionalKey("P", "Z", 10000000L, 100000000L);
        List<Row> rows = Arrays.asList(row1, row2, row3, row4);

        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildList());

        ingestData(instanceProperties, stateStore, tableProperties, rows.iterator());

        // Split the root partition into 2:
        PartitionTree partialTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "I")
                .buildTree();
        update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(partialTree.getPartition("root"),
                partialTree.getPartition("left"), partialTree.getPartition("right"));

        ingestData(instanceProperties, stateStore, tableProperties, rows.iterator());

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "I")
                .splitToNewChildrenOnDimension("left", "P1", "P3", 1, "T")
                .splitToNewChildrenOnDimension("right", "P2", "P4", 1, "T")
                .buildTree();
        update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("left"),
                tree.getPartition("P1"), tree.getPartition("P3"));
        update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("right"),
                tree.getPartition("P2"), tree.getPartition("P4"));
        ingestData(instanceProperties, stateStore, tableProperties, rows.iterator());

        List<String> filesInLeafPartition1 = stateStore.getFileReferences().stream()
                .filter(f -> List.of("P1", "left", "root").contains(f.getPartitionId()))
                .map(FileReference::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition2 = stateStore.getFileReferences().stream()
                .filter(f -> List.of("P2", "right", "root").contains(f.getPartitionId()))
                .map(FileReference::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition3 = stateStore.getFileReferences().stream()
                .filter(f -> List.of("P3", "left", "root").contains(f.getPartitionId()))
                .map(FileReference::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition4 = stateStore.getFileReferences().stream()
                .filter(f -> List.of("P4", "right", "root").contains(f.getPartitionId()))
                .map(FileReference::getFilename)
                .collect(Collectors.toList());

        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1 - query for entire space
        Range range1 = rangeFactory.createRange(field1, "", true, null, false);
        Range range2 = rangeFactory.createRange(field2, "", true, null, false);
        Region region = new Region(Arrays.asList(range1, range2));

        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable().hasSize(12) // 12 because the same data was added 3 times at different levels of the tree
                    .hasSameElementsAs(rows);
        }

        // When 2 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", true, "H", true);
        range2 = rangeFactory.createRange(field2, "", true, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(row1));
        }

        // When 3 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", true, "H", false);
        range2 = rangeFactory.createRange(field2, "", true, "S", false);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(row1));
        }

        // When 4 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", false, "H", true);
        range2 = rangeFactory.createRange(field2, "", false, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 4
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(row1));
        }

        // When 5 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", false, "H", false);
        range2 = rangeFactory.createRange(field2, "", false, "S", false);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 5
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(row1));
        }

        // When 6 - query for range within partitions 1 and 2
        range1 = rangeFactory.createRange(field1, "", true, "Z", true);
        range2 = rangeFactory.createRange(field2, "", true, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 6
            assertThat(results).toIterable().hasSize(6)
                    .hasSameElementsAs(Arrays.asList(row1, row2));
        }

        // When 7 - query for range to the right of the data in partitions 2 and 4
        range1 = rangeFactory.createRange(field1, "T", true, "Z", true);
        range2 = rangeFactory.createRange(field2, "", true, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 7
            assertThat(results).isExhausted();
        }

        // When 8 - query for a 1-dimensional range
        range1 = rangeFactory.createRange(field1, "J", true, "Z", true);
        region = new Region(range1);
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 8
            assertThat(results).toIterable().hasSize(6)
                    .hasSameElementsAs(Arrays.asList(row2, row4));
        }

        // When 9 - query for a range where the first dimension is constant
        range1 = rangeFactory.createExactRange(field1, "C");
        range2 = rangeFactory.createRange(field2, "", true, null, true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 9
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(row3));
        }

        // When 10 - query for a range where the max equals row1 and max is not inclusive
        range1 = rangeFactory.createRange(field1, "", true, "D", false);
        range2 = rangeFactory.createRange(field2, "", true, "T", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 10
            assertThat(results).isExhausted();
        }

        // When 11 - query for a range where the max equals row1 and max is inclusive
        range1 = rangeFactory.createRange(field1, "", true, "D", true);
        range2 = rangeFactory.createRange(field2, "", true, "T", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 11
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(row1));
        }

        // When 12 - query for a range where the boundaries cover all 4 rows, min is inclusive, max is not inclusive
        // Row i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - no
        range1 = rangeFactory.createRange(field1, "C", true, "P", false);
        range2 = rangeFactory.createRange(field2, "H", true, "Z", false);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 12
            assertThat(results).toIterable().hasSize(9)
                    .hasSameElementsAs(Arrays.asList(row1, row2, row3));
        }

        // When 13 - query for a range where the boundaries cover all 4 rows, min is inclusive, and max is inclusive
        // Row i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - yes
        range1 = rangeFactory.createRange(field1, "C", true, "P", true);
        range2 = rangeFactory.createRange(field2, "H", true, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 13
            assertThat(results).toIterable().hasSize(12)
                    .hasSameElementsAs(Arrays.asList(row1, row2, row3, row4));
        }

        // When 14 - query for a range where the boundaries cover all 4 rows, min is not inclusive, and max is not inclusive
        // Row i is in range? 1 - yes; 2 - no; 3 - no; 4 - no
        range1 = rangeFactory.createRange(field1, "C", false, "P", false);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", false);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 14
            assertThat(results).toIterable().hasSize(3)
                    .hasSameElementsAs(Collections.singletonList(row1));
        }

        // When 15 - query for a range where the boundaries cover all 4 rows, min is not inclusive, and max is inclusive
        // Row i is in range? 1 - yes; 2 - no; 3 - no; 4 - yes
        range1 = rangeFactory.createRange(field1, "C", false, "P", true);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 15
            assertThat(results).toIterable().hasSize(6)
                    .hasSameElementsAs(Arrays.asList(row1, row4));
        }

        // When 16
        range1 = rangeFactory.createRange(field1, "C", false, "P", true);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        Query query = queryWithRegion(region);
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 16
        assertThat(leafPartitionQueries)
                .usingRecursiveFieldByFieldElementComparator(RecursiveComparisonConfiguration.builder()
                        .withIgnoredFields("subQueryId")
                        .withIgnoredCollectionOrderInFields("files")
                        .build())
                .containsExactlyInAnyOrder(
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("P1")
                                .partitionRegion(tree.getPartition("P1").getRegion())
                                .files(filesInLeafPartition1)
                                .build(),
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("P2")
                                .partitionRegion(tree.getPartition("P2").getRegion())
                                .files(filesInLeafPartition2)
                                .build(),
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("P3")
                                .partitionRegion(tree.getPartition("P3").getRegion())
                                .files(filesInLeafPartition3)
                                .build(),
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("P4")
                                .partitionRegion(tree.getPartition("P4").getRegion())
                                .files(filesInLeafPartition4)
                                .build());
    }

    @Test
    public void shouldReturnDataCorrectlySorted() throws IOException, IteratorCreationException, ObjectFactoryException, QueryException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder()
                .rowKeyFields(field)
                .sortKeyFields(new Field("value1", new LongType()))
                .valueFields(new Field("value2", new LongType()))
                .build();
        tableProperties.setSchema(schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildList());
        ingestData(instanceProperties, stateStore, tableProperties, getMultipleRowsForTestingSorting().iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable()
                    .containsExactlyElementsOf(getMultipleRowsForTestingSorting()
                            .stream()
                            .filter(r -> (long) r.get("key") == 1L)
                            .sorted(Comparator.comparing(r -> (Long) r.get("value1")))
                            .collect(Collectors.toList()));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 5L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).toIterable()
                    .containsExactlyElementsOf(getMultipleRowsForTestingSorting()
                            .stream()
                            .filter(r -> ((long) r.get("key")) == 5L)
                            .sorted(Comparator.comparing(r -> (Long) r.get("value1")))
                            .collect(Collectors.toList()));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, 0L));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).isExhausted();
        }
    }

    @Test
    public void shouldReturnCorrectDataWhenOneRowInOneFileInOnePartitionAndCompactionIteratorApplied() throws IOException, IteratorCreationException, ObjectFactoryException, QueryException {
        // Given
        Field field = new Field("id", new StringType());
        Schema schema = Schema.builder()
                .rowKeyFields(field)
                .valueFields(new Field("timestamp", new LongType()))
                .build();
        tableProperties.setSchema(schema);
        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema).rootFirst("root").buildList());
        List<Row> rows = getRowsForAgeOffIteratorTest();
        ingestData(instanceProperties, stateStore, tableProperties, rows.iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, "1"));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 1
            assertThat(results).toIterable().containsExactly(rows.get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, "0"));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 2
            assertThat(results).isExhausted();
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, "2"));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 3
            assertThat(results).isExhausted();
        }

        // When 4
        region = new Region(rangeFactory.createExactRange(field, "3"));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 4
            assertThat(results).isExhausted();
        }

        // When 5
        region = new Region(rangeFactory.createExactRange(field, "4"));
        try (CloseableIterator<Row> results = queryExecutor.execute(queryWithRegion(region))) {

            // Then 5
            assertThat(results).toIterable().containsExactly(rows.get(3));
        }
    }

    @Test
    public void shouldReturnCorrectDataWhenQueryTimeIteratorApplied() throws IteratorCreationException, IOException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getSecurityLabelSchema();
        Field field = schema.getRowKeyFields().get(0);
        tableProperties.setSchema(schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildList());
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, tableProperties,
                    getRowsForQueryTimeIteratorTest(i % 2 == 0 ? "notsecret" : "secret").iterator());
        }
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = Query.builder()
                .tableName("myTable")
                .queryId("id")
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName(SecurityFilteringIterator.class.getName())
                        .queryTimeIteratorConfig("securityLabel,notsecret")
                        .build())
                .build();
        try (CloseableIterator<Row> results = queryExecutor.execute(query)) {

            // Then
            Row expected = getRowsForQueryTimeIteratorTest("notsecret").get(0);
            assertThat(results).toIterable().hasSize(5)
                    .allSatisfy(result -> assertThat(result).isEqualTo(expected));
        }
    }

    @Test
    public void shouldReturnOnlyRequestedValuesWhenSpecified() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        tableProperties.setSchema(schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema).rootFirst("root").buildList());
        ingestData(instanceProperties, stateStore, tableProperties, getRows().iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = Query.builder()
                .tableName("unused")
                .queryId("abc")
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .requestedValueFields(Lists.newArrayList("value2"))
                        .build())
                .build();
        try (CloseableIterator<Row> results = queryExecutor.execute(query)) {

            // Then
            assertThat(results).toIterable().hasSize(1)
                    .flatExtracting(Row::getKeys).doesNotContain("value1").contains("key", "value2");
        }
    }

    @Test
    public void shouldIncludeFieldsRequiredByIteratorsEvenIfNotSpecifiedByTheUser() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getSecurityLabelSchema();
        Field field = schema.getRowKeyFields().get(0);
        tableProperties.setSchema(schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema).rootFirst("root").buildList());
        ingestData(instanceProperties, stateStore, tableProperties, getRowsForQueryTimeIteratorTest("secret").iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = Query.builder()
                .tableName("unused")
                .queryId("abc")
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName(SecurityFilteringIterator.class.getName())
                        .queryTimeIteratorConfig("securityLabel,secret")
                        .requestedValueFields(Lists.newArrayList("value"))
                        .build())
                .build();
        try (CloseableIterator<Row> results = queryExecutor.execute(query)) {

            // Then
            assertThat(results).hasNext().toIterable().allSatisfy(result -> assertThat(result.getKeys()).contains("key", "value", "securityLabel"));
        }
    }

    private RangeFactory rangeFactory() {
        return new RangeFactory(tableProperties.getSchema());
    }

    private StateStore initialiseStateStore(TableProperties tableProperties, List<Partition> partitions) {
        return InMemoryTransactionLogStateStore.createAndInitialiseWithPartitions(partitions, tableProperties, new InMemoryTransactionLogs());
    }

    private QueryExecutor queryExecutor(TableProperties tableProperties, StateStore stateStore) {
        return new QueryExecutor(ObjectFactory.noUserJars(),
                tableProperties, stateStore,
                new QueryEngineSelector(executorService, new Configuration()).getRowRetriever(tableProperties));
    }

    private QueryExecutor initQueryExecutor() {
        QueryExecutor executor = new QueryExecutor(ObjectFactory.noUserJars(),
                tableProperties, stateStore,
                new QueryEngineSelector(executorService, new Configuration()).getRowRetriever(tableProperties));
        executor.init();
        return executor;
    }

    private Query queryWithRegion(Region region) {
        return Query.builder()
                .tableName("myTable")
                .queryId("id")
                .regions(List.of(region))
                .build();
    }

    private Schema getLongKeySchema() {
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

    private void ingestData(Iterator<Row> rowIterator) throws IOException, IteratorCreationException {
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        IngestFactory factory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(tempDir, null).toString())
                .instanceProperties(instanceProperties)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .hadoopConfiguration(new Configuration())
                .build();
        factory.ingestFromRowIterator(tableProperties, rowIterator);
    }

    private void ingestData(InstanceProperties instanceProperties, StateStore stateStore,
            TableProperties tableProperties, Iterator<Row> rowIterator) throws IOException, IteratorCreationException {
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        IngestFactory factory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(tempDir, null).toString())
                .instanceProperties(instanceProperties)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .hadoopConfiguration(new Configuration())
                .build();
        factory.ingestFromRowIterator(tableProperties, rowIterator);
    }

    private Region rootPartitionRegion() {
        return new PartitionTree(stateStore.getAllPartitions()).getRootPartition().getRegion();
    }

    private List<String> filenamesInPartition(String partitionId) {
        return stateStore.getFileReferences().stream()
                .filter(f -> f.getPartitionId().equals(partitionId))
                .map(FileReference::getFilename)
                .toList();
    }

    private List<Row> getRows() {
        List<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.put("key", 1L);
        row.put("value1", 10L);
        row.put("value2", 100L);
        rows.add(row);
        return rows;
    }

    protected List<Row> getMultipleIdenticalRows() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows.addAll(getRows());
        }
        return rows;
    }

    protected List<Row> getMultipleRows() {
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Row row = new Row();
            row.put("key", (long) i);
            row.put("value1", i * 10L);
            row.put("value2", i * 100L);
            rows.add(row);
        }
        return rows;
    }

    protected List<Row> getMultipleRowsMultidimRowKey() {
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Row row = new Row();
            row.put("key1", (long) i);
            row.put("key2", String.valueOf(i));
            row.put("value1", i * 10L);
            row.put("value2", i * 100L);
            rows.add(row);
        }
        return rows;
    }

    protected List<Row> getMultipleRowsForTestingSorting() {
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            for (int j = 1000; j >= 900; j--) {
                Row row = new Row();
                row.put("key", (long) i);
                row.put("value1", (long) j);
                row.put("value2", i * 100L);
                rows.add(row);
            }
        }
        return rows;
    }

    protected List<Row> getRowsForAgeOffIteratorTest() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("id", "1");
        row1.put("timestamp", System.currentTimeMillis());
        rows.add(row1);
        Row row2 = new Row();
        row2.put("id", "2");
        row2.put("timestamp", System.currentTimeMillis() - 1_000_000_000L);
        rows.add(row2);
        Row row3 = new Row();
        row3.put("id", "3");
        row3.put("timestamp", System.currentTimeMillis() - 2_000_000L);
        rows.add(row3);
        Row row4 = new Row();
        row4.put("id", "4");
        row4.put("timestamp", System.currentTimeMillis());
        rows.add(row4);
        return rows;
    }

    protected List<Row> getRowsForQueryTimeIteratorTest(String securityLabel) {
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Row row = new Row();
            row.put("key", (long) i);
            row.put("value", i * 10L);
            row.put("securityLabel", securityLabel);
            rows.add(row);
        }
        return rows;
    }

    private static Row createRowMultidimensionalKey(String key1, String key2, long value1, long value2) {
        Row row = new Row();
        row.put("key1", key1);
        row.put("key2", key2);
        row.put("value1", value1);
        row.put("value2", value2);
        return row;
    }
}
