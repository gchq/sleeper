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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.AgeOffIterator;
import sleeper.core.iterator.closeable.CloseableIterator;
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
import sleeper.example.iterator.SecurityFilteringIterator;
import sleeper.ingest.runner.IngestFactory;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.rowretrieval.QueryExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

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

    @Nested
    @DisplayName("No files")
    class NoFiles {

        @BeforeEach
        void setUp() {
            tableProperties.setSchema(getLongKeySchema());
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
        }

        @Test
        void shouldQueryByExactRange() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 1L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldQueryByRange() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // When
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));

            // When / Then
            assertThat(initQueryExecutor()
                    .splitIntoLeafPartitionQueries(queryWithRegion(region)))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("One row, one file, one partition")
    class OneRow {

        Row row = new Row(Map.of(
                "key", 1L,
                "value1", 10L,
                "value2", 100L));

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(getLongKeySchema());
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            ingestData(List.of(row));
        }

        @Test
        void shouldFindRowByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 1L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactly(row);
            }
        }

        @Test
        void shouldFindNothingByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 0L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldFindRowByRange() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", -10L, true, 1L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactly(row);
            }
        }

        @Test
        void shouldFindNothingByRange() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 10L, true, 100L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));
            Query query = queryWithRegion(region);
            List<String> files = filenamesInPartition("root");

            // When / Then
            assertThat(initQueryExecutor().splitIntoLeafPartitionQueries(query))
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
    }

    @Nested
    @DisplayName("Multiple identical rows, one file, one partition")
    class MultipleIdenticalRows {
        Row row = new Row(Map.of(
                "key", 1L,
                "value1", 10L,
                "value2", 100L));
        List<Row> rows = IntStream.range(0, 10).mapToObj(i -> row).toList();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(getLongKeySchema());
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            ingestData(rows);
        }

        @Test
        void shouldReturnAllRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 1L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactlyElementsOf(rows);
            }
        }

        @Test
        void shouldReturnNoRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 0L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldReturnAllRowsByRange() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactlyElementsOf(rows);
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));
            Query query = queryWithRegion(region);
            List<String> files = filenamesInPartition("root");

            // When / Then
            assertThat(initQueryExecutor().splitIntoLeafPartitionQueries(query))
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
    }

    @Nested
    @DisplayName("Multiple identical files, one partition, one row each")
    class MultipleIdenticalSingleRowFiles {
        Row row = new Row(Map.of(
                "key", 1L,
                "value1", 10L,
                "value2", 100L));
        List<Row> rows = IntStream.range(0, 10).mapToObj(i -> row).toList();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(getLongKeySchema());
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            for (int i = 0; i < 10; i++) {
                ingestData(List.of(row));
            }
        }

        @Test
        void shouldReturnAllRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 1L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactlyElementsOf(rows);
            }
        }

        @Test
        void shouldReturnNoRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 0L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldReturnAllRowsByRange() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactlyElementsOf(rows);
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));
            Query query = queryWithRegion(region);
            List<String> files = filenamesInPartition("root");

            // When / Then
            assertThat(initQueryExecutor().splitIntoLeafPartitionQueries(query))
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
    }

    @Nested
    @DisplayName("Multiple identical files, one partition, multiple rows each")
    class MultipleIdenticalMultiRowFiles {

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(getLongKeySchema());
            update(stateStore).initialise(tableProperties);
            for (int i = 0; i < 10; i++) {
                ingestData(getMultipleRows());
            }
        }

        @Test
        void shouldReturnFirstRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 1L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(10)
                        .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(0)));
            }
        }

        @Test
        void shouldReturnMiddleRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 5L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(10)
                        .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(4)));
            }
        }

        @Test
        void shouldReturnNoRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 0L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldReturnAllRowsByExactRange() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(100)
                        .hasSameElementsAs(getMultipleRows());
            }
        }

        @Test
        void shouldReturnAllRowsByRangeContainingRangeOfData() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", -100000L, true, 123456789L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(100)
                        .hasSameElementsAs(getMultipleRows());
            }
        }

        @Test
        void shouldReturnSomeRowsByRangePartiallyCoveringData() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 5L, true, 123456789L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(60)
                        .hasSameElementsAs(getMultipleRows().stream()
                                .filter(r -> ((long) r.get("key")) >= 5L)
                                .collect(Collectors.toList()));
            }
        }

        @Test
        void shouldExcludeLastValueWhenMaxIsNotInclusive() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(90)
                        .hasSameElementsAs(getMultipleRows().stream()
                                .filter(r -> ((long) r.get("key")) >= 1L && ((long) r.get("key")) < 10L)
                                .collect(Collectors.toList()));
            }
        }

        @Test
        void shouldExcludeFirstValueWhenMinIsNotInclusive() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, false, 10L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then 7
                assertThat(results).toIterable().hasSize(90)
                        .hasSameElementsAs(getMultipleRows().stream()
                                .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) <= 10L)
                                .collect(Collectors.toList()));
            }
        }

        @Test
        void shouldExcludeFirstAndLastValueWhenMaxAndMinAreNotInclusive() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, false, 10L, false));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(80)
                        .hasSameElementsAs(getMultipleRows().stream()
                                .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) < 10L)
                                .collect(Collectors.toList()));
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));
            Query query = queryWithRegion(region);
            List<String> files = filenamesInPartition("root");

            // When / Then
            assertThat(initQueryExecutor().splitIntoLeafPartitionQueries(query))
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
    }

    @Nested
    @DisplayName("Multiple partitions with identical data in multiple files")
    class MultiplePartitionsMultipleFiles {

        Schema schema = getLongKeySchema();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildTree();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            update(stateStore).initialise(tree);
            for (int i = 0; i < 10; i++) {
                ingestData(getMultipleRows());
            }
        }

        @Test
        void shouldReturnFirstRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 1L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(10)
                        .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(0)));
            }
        }

        @Test
        void shouldReturnMiddleRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 5L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(10)
                        .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRows().get(4)));
            }
        }

        @Test
        void shouldReturnNoRowsByExactMatch() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 0L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldReturnAllRowsByRangeContainingRangeOfData() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", -100000L, true, 123456789L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(100)
                        .hasSameElementsAs(getMultipleRows());
            }
        }

        @Test
        void shouldReturnSomeRowsByRangePartiallyCoveringData() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 5L, true, 123456789L, true));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(60)
                        .hasSameElementsAs(getMultipleRows()
                                .stream().filter(r -> ((long) r.get("key")) >= 5L)
                                .collect(Collectors.toList()));
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // Given
            Region region = new Region(rangeFactory().createRange("key", 1L, true, 10L, false));
            Query query = queryWithRegion(region);
            List<String> filesInLeftPartition = filenamesInPartition("left");
            List<String> filesInRightPartition = filenamesInPartition("right");

            // When / Then
            assertThat(initQueryExecutor().splitIntoLeafPartitionQueries(query))
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
    }

    @Nested
    @DisplayName("Multidimensional row key with one split point")
    class MultidimensionalRowKeyOneSplitPoint {

        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new LongType()), new Field("key2", new StringType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildTree();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            update(stateStore).initialise(tree);
            for (int i = 0; i < 10; i++) {
                ingestData(getMultipleRowsMultidimRowKey());
            }
        }

        @Test
        void shouldReturnFirstRowsByExactMatch() throws Exception {
            // Given
            Range range1 = rangeFactory().createExactRange("key1", 1L);
            Range range2 = rangeFactory().createExactRange("key2", "1");
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(10)
                        .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRowsMultidimRowKey().get(0)));
            }
        }

        @Test
        void shouldReturnMiddleRowsByExactMatch() throws Exception {
            // Given
            Range range1 = rangeFactory().createExactRange("key1", 5L);
            Range range2 = rangeFactory().createExactRange("key2", "5");
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(10)
                        .allSatisfy(row -> assertThat(row).isEqualTo(getMultipleRowsMultidimRowKey().get(4)));
            }
        }

        @Test
        void shouldReturnNoRowsByExactMatch() throws Exception {
            // Given
            Range range1 = rangeFactory().createExactRange("key1", 8L);
            Range range2 = rangeFactory().createExactRange("key2", "notthere");
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldReturnAllRowsByRangeContainingRangeOfData() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", -100000L, true, 123456789L, true);
            Range range2 = rangeFactory().createRange("key2", "0", true, "99999999999", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(100)
                        .hasSameElementsAs(getMultipleRowsMultidimRowKey());
            }
        }

        @Test
        void shouldReturnSomeRowsByRangePartiallyCoveringData() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", 2L, true, 5L, true);
            Range range2 = rangeFactory().createRange("key2", "3", true, "6", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(30)
                        .hasSameElementsAs(getMultipleRowsMultidimRowKey().stream()
                                .filter(r -> ((long) r.get("key1")) >= 2L && ((long) r.get("key1")) <= 5L)
                                .filter(r -> ((String) r.get("key2")).compareTo("3") >= 0
                                        && ((String) r.get("key2")).compareTo("6") <= 0)
                                .collect(Collectors.toList()));
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // Given
            Range range1 = rangeFactory().createRange("key1", 2L, true, 500L, true);
            Range range2 = rangeFactory().createRange("key2", "3", true, "6", true);
            Region region = new Region(Arrays.asList(range1, range2));
            Query query = queryWithRegion(region);
            List<String> filesInLeftPartition = filenamesInPartition("left");
            List<String> filesInRightPartition = filenamesInPartition("right");

            // When / Then
            assertThat(initQueryExecutor().splitIntoLeafPartitionQueries(query))
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
    }

    @Nested
    @DisplayName("Data in multiple levels of multidimensional partition tree")
    class MultiplePartitionTreeLevelsAndDimensions {

        Schema schema = Schema.builder()
                .rowKeyFields(
                        new Field("key1", new StringType()),
                        new Field("key2", new StringType()))
                .valueFields(
                        new Field("value1", new LongType()),
                        new Field("value2", new LongType()))
                .build();
        //  Partitions:
        //  - Root partition covers the whole space
        //  - Root has 2 children: one is 1 and 3 below, the other is 2 and 4
        //  - There are 4 leaf partitions:
        //      null +-----------+-----------+
        //           |     3     |    4      |
        //       "T" +-----------+-----------+
        //           |     1     |    2      |
        //        "" +-----------+-----------+
        //           ""         "I"          null      (Dimension 1)
        // Add 4 rows - row i is in the center of partition i
        Row row1 = createRowMultidimensionalKey("D", "J", 10L, 100L);
        Row row2 = createRowMultidimensionalKey("K", "H", 1000L, 10000L);
        Row row3 = createRowMultidimensionalKey("C", "X", 100000L, 1000000L);
        Row row4 = createRowMultidimensionalKey("P", "Z", 10000000L, 100000000L);
        List<Row> rows = List.of(row1, row2, row3, row4);
        PartitionTree rootOnlyTree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        PartitionTree partialTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "I")
                .buildTree();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "I")
                .splitToNewChildrenOnDimension("left", "P1", "P3", 1, "T")
                .splitToNewChildrenOnDimension("right", "P2", "P4", 1, "T")
                .buildTree();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            update(stateStore).initialise(rootOnlyTree);
            ingestData(rows);
            update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(partialTree.getPartition("root"),
                    partialTree.getPartition("left"), partialTree.getPartition("right"));
            ingestData(rows);
            update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("left"),
                    tree.getPartition("P1"), tree.getPartition("P3"));
            update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("right"),
                    tree.getPartition("P2"), tree.getPartition("P4"));
            ingestData(rows);
        }

        @Test
        void shouldReturnAllRowsByRange() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "", true, null, false);
            Range range2 = rangeFactory().createRange("key2", "", true, null, false);
            Region region = new Region(List.of(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        // 12 because the same data was added 3 times at different levels of the tree
                        .hasSize(12)
                        .hasSameElementsAs(rows);
            }
        }

        @Test
        void shouldFindFirstRowsByRange() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "C", true, "E", true);
            Range range2 = rangeFactory().createRange("key2", "I", true, "K", true);
            Region region = new Region(List.of(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactly(row1, row1, row1);
            }
        }

        @Test
        void shouldFindFirstRowsByExactMatchOnBothKeys() throws Exception {
            // Given
            Range range1 = rangeFactory().createExactRange("key1", "D");
            Range range2 = rangeFactory().createExactRange("key2", "J");
            Region region = new Region(List.of(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactly(row1, row1, row1);
            }
        }

        @Test
        void shouldExcludeFirstRowsByKey1MinNotInclusive() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "D", false, "E", true);
            Range range2 = rangeFactory().createRange("key2", "I", true, "K", true);
            Region region = new Region(List.of(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldExcludeFirstRowsByKey1MaxNotInclusive() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "C", true, "D", false);
            Range range2 = rangeFactory().createRange("key2", "I", true, "K", true);
            Region region = new Region(List.of(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldExcludeFirstRowsByKey2MinNotInclusive() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "C", true, "E", true);
            Range range2 = rangeFactory().createRange("key2", "J", false, "K", true);
            Region region = new Region(List.of(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldExcludeFirstRowsByKey2MaxNotInclusive() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "C", true, "E", true);
            Range range2 = rangeFactory().createRange("key2", "I", true, "J", false);
            Region region = new Region(List.of(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldQueryPartition1And2ByRange() throws Exception {

            // Given
            Range range1 = rangeFactory().createRange("key1", "", true, "Z", true);
            Range range2 = rangeFactory().createRange("key2", "", true, "S", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(6)
                        .hasSameElementsAs(Arrays.asList(row1, row2));
            }
        }

        @Test
        void shouldFindNoRowsInRegionToRightOfAllData() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "T", true, "Z", true);
            Range range2 = rangeFactory().createRange("key2", "", true, "Z", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldQueryByOneDimensionalRegion() throws Exception {
            // Given
            Range range = rangeFactory().createRange("key1", "J", true, "Z", true);
            Region region = new Region(range);

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(6)
                        .hasSameElementsAs(List.of(row2, row4));
            }
        }

        @Test
        void shouldQueryByRegionWithExactMatchOnFirstDimension() throws Exception {
            // Given
            Range range1 = rangeFactory().createExactRange("key1", "C");
            Range range2 = rangeFactory().createRange("key2", "", true, null, true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(3)
                        .hasSameElementsAs(List.of(row3));
            }
        }

        @Test
        void shouldFindNoRowsInRegionWhereMaxEqualsRow1AndMaxIsNotInclusive() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "", true, "D", false);
            Range range2 = rangeFactory().createRange("key2", "", true, "T", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldFindFirstRowsInRegionWhereMaxEqualsRow1AndMaxIsInclusive() throws Exception {
            // Given
            Range range1 = rangeFactory().createRange("key1", "", true, "D", true);
            Range range2 = rangeFactory().createRange("key2", "", true, "T", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(3)
                        .hasSameElementsAs(List.of(row1));
            }
        }

        @Test
        void shouldExcludeLastRowsWhenRegionCoversAllRowsWithMaxEqualToLastRowAndNotInclusive() throws Exception {
            // Given
            // Row i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - no
            Range range1 = rangeFactory().createRange("key1", "C", true, "P", false);
            Range range2 = rangeFactory().createRange("key2", "H", true, "Z", false);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(9)
                        .hasSameElementsAs(Arrays.asList(row1, row2, row3));
            }
        }

        @Test
        void shouldReturnAllRowsWhenRegionCoversAllRowsWithMaxEqualToLastRowAndIsInclusive() throws Exception {
            // Given
            // Row i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - yes
            Range range1 = rangeFactory().createRange("key1", "C", true, "P", true);
            Range range2 = rangeFactory().createRange("key2", "H", true, "Z", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(12)
                        .hasSameElementsAs(Arrays.asList(row1, row2, row3, row4));
            }
        }

        // Row row1 = createRowMultidimensionalKey("D", "J", 10L, 100L);
        // Row row2 = createRowMultidimensionalKey("K", "H", 1000L, 10000L);
        // Row row3 = createRowMultidimensionalKey("C", "X", 100000L, 1000000L);
        // Row row4 = createRowMultidimensionalKey("P", "Z", 10000000L, 100000000L);
        @Test
        void shouldExcludeRowsAtEdgeOfRangesWhenRegionCoversAllRowsWithBoundsNotInclusive() throws Exception {
            // Given
            // Row i is in range? 1 - yes; 2 - excluded by key2 min; 3 - excluded by key1 min; 4 - excluded by key1 max and key2 max
            Range range1 = rangeFactory().createRange("key1", "C", false, "P", false);
            Range range2 = rangeFactory().createRange("key2", "H", false, "Z", false);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(3)
                        .hasSameElementsAs(Collections.singletonList(row1));
            }

        }

        @Test
        void shouldIncludeRowsAtMaxOfRangesWhenRegionCoversAllRowsWithMinExclusiveMaxInclusive() throws Exception {
            // Given
            // Row i is in range? 1 - yes; 2 - excluded by key2 min; 3 - excluded by key1 min; 4 - yes
            Range range1 = rangeFactory().createRange("key1", "C", false, "P", true);
            Range range2 = rangeFactory().createRange("key2", "H", false, "Z", true);
            Region region = new Region(Arrays.asList(range1, range2));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().hasSize(6)
                        .hasSameElementsAs(Arrays.asList(row1, row4));
            }
        }

        @Test
        void shouldSplitQueryByRange() {
            // Given
            Range range1 = rangeFactory().createRange("key1", "C", false, "P", true);
            Range range2 = rangeFactory().createRange("key2", "H", false, "Z", true);
            Region region = new Region(List.of(range1, range2));
            Query query = queryWithRegion(region);
            List<String> filesInLeafPartition1 = filenamesInPartition("P1", "left", "root");
            List<String> filesInLeafPartition2 = filenamesInPartition("P2", "right", "root");
            List<String> filesInLeafPartition3 = filenamesInPartition("P3", "left", "root");
            List<String> filesInLeafPartition4 = filenamesInPartition("P4", "right", "root");

            // When / Then
            assertThat(initQueryExecutor().splitIntoLeafPartitionQueries(query))
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

    }

    @Nested
    @DisplayName("Return sorted data")
    class ReturnSortedData {

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new LongType()))
                    .sortKeyFields(new Field("value1", new LongType()))
                    .valueFields(new Field("value2", new LongType()))
                    .build());
            update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 5L)
                    .buildList());
            ingestData(getMultipleRowsForTestingSorting());
        }

        @Test
        void shouldReturnSortedDataByFirstKeyValue() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 1L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactlyElementsOf(getSortedRowsForTestingSortingWithKey(1));
            }
        }

        @Test
        void shouldReturnSortedDataByMidKeyValue() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 5L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable()
                        .containsExactlyElementsOf(getSortedRowsForTestingSortingWithKey(5));
            }
        }

        @Test
        void shouldReturnNoDataByKeyWithNoData() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("key", 0L));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }
    }

    @Nested
    @DisplayName("Apply compaction iterator")
    class ApplyCompactionIterator {
        Row row1 = new Row(Map.of("id", "1", "timestamp", System.currentTimeMillis()));
        Row row2 = new Row(Map.of("id", "2", "timestamp", System.currentTimeMillis() - 1_000_000_000L));
        Row row3 = new Row(Map.of("id", "3", "timestamp", System.currentTimeMillis() - 2_000_000L));
        Row row4 = new Row(Map.of("id", "4", "timestamp", System.currentTimeMillis()));

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("id", new StringType()))
                    .valueFields(new Field("timestamp", new LongType()))
                    .build());
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            ingestData(List.of(row1, row2, row3, row4));
            tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
            tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");
        }

        @Test
        void shouldFindRowNotAgedOffAtCurrentTime() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("id", "1"));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().containsExactly(row1);
            }
        }

        @Test
        void shouldNotFindRowThatWasNotWritten() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("id", "0"));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldNotFindRowThatAgedOffALongTimeAgo() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("id", "2"));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldNotFindRowThatAgedOffSomeTimeAgo() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("id", "3"));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).isExhausted();
            }
        }

        @Test
        void shouldFindRowAfterRowsThatAgedOff() throws Exception {
            // Given
            Region region = new Region(rangeFactory().createExactRange("id", "4"));

            // When
            try (CloseableIterator<Row> results = initQueryExecutor().execute(queryWithRegion(region))) {

                // Then
                assertThat(results).toIterable().containsExactly(row4);
            }
        }
    }

    @Test
    public void shouldApplyQueryTimeIterator() throws Exception {
        // Given
        tableProperties.setSchema(getSecurityLabelSchema());
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildList());
        for (int i = 0; i < 10; i++) {
            ingestData(getRowsForQueryTimeIteratorTest(i % 2 == 0 ? "notsecret" : "secret").iterator());
        }
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
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
    public void shouldReturnOnlyRequestedValuesWhenSpecified() throws Exception {
        // Given
        tableProperties.setSchema(getLongKeySchema());
        update(stateStore).initialise(tableProperties);
        ingestData(getRows().iterator());
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
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
    public void shouldIncludeFieldsRequiredByIteratorsEvenIfNotSpecifiedByTheUser() throws Exception {
        // Given
        tableProperties.setSchema(getSecurityLabelSchema());
        update(stateStore).initialise(tableProperties);
        ingestData(getRowsForQueryTimeIteratorTest("secret").iterator());
        QueryExecutor queryExecutor = initQueryExecutor();
        RangeFactory rangeFactory = rangeFactory();

        // When
        Region region = new Region(rangeFactory.createExactRange("key", 1L));
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
            assertThat(results).hasNext().toIterable()
                    .allSatisfy(result -> assertThat(result.getKeys()).contains("key", "value", "securityLabel"));
        }
    }

    private RangeFactory rangeFactory() {
        return new RangeFactory(tableProperties.getSchema());
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

    private Schema getSecurityLabelSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value", new LongType()), new Field("securityLabel", new StringType()))
                .build();
    }

    private void ingestData(List<Row> rows) throws Exception {
        ingestData(rows.iterator());
    }

    private void ingestData(Iterator<Row> rowIterator) throws Exception {
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

    private List<String> filenamesInPartition(String... partitionIds) {
        Set<String> ids = new HashSet<>(List.of(partitionIds));
        return stateStore.getFileReferences().stream()
                .filter(f -> ids.contains(f.getPartitionId()))
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

    private List<Row> getMultipleRows() {
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

    private List<Row> getMultipleRowsMultidimRowKey() {
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

    private List<Row> getMultipleRowsForTestingSorting() {
        return LongStream.rangeClosed(1, 10)
                .mapToObj(i -> LongStream.rangeClosed(0, 100)
                        .map(n -> 1000 - n)
                        .mapToObj(j -> rowForTestingSorting(i, j)))
                .flatMap(stream -> stream)
                .toList();
    }

    private List<Row> getSortedRowsForTestingSortingWithKey(long i) {
        return LongStream.rangeClosed(900, 1000)
                .mapToObj(j -> rowForTestingSorting(i, j))
                .toList();
    }

    private Row rowForTestingSorting(long i, long j) {
        return new Row(Map.of(
                "key", i,
                "value1", j,
                "value2", i * 100));
    }

    private List<Row> getRowsForAgeOffIteratorTest() {
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

    private List<Row> getRowsForQueryTimeIteratorTest(String securityLabel) {
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
