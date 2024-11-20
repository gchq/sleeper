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
package sleeper.query.core.recordretrieval;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.iterator.impl.SecurityFilteringIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryProcessingConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class QueryExecutorTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final InMemoryDataStore recordStore = new InMemoryDataStore();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);

    @Nested
    @DisplayName("Query records")
    class QueryRecords {

        @Test
        void shouldReturnSubRangeInSinglePartition() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When
            List<Record> records = getRecords(queryRange(100L, 200L));

            // Then
            assertThat(records).containsExactly(new Record(Map.of("key", 123L)));
        }

        @Test
        void shouldReturnRecordInOneOfTwoRanges() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When
            List<Record> records = getRecords(queryRegions(range(100L, 200L), range(400L, 500L)));

            // Then
            assertThat(records).containsExactly(new Record(Map.of("key", 123L)));
        }

        @Test
        void shouldNotFindRecordOutsideSubRangeInSinglePartition() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When
            List<Record> records = getRecords(queryRange(200L, 300L));

            // Then
            assertThat(records).isEmpty();
        }

        @Test
        void shouldNotFindRecordOutsidePartitionRangeWhenFileContainsAnInactiveRecord() throws Exception {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 5L)
                    .buildList());
            addPartitionFile("R", "file.parquet", List.of(
                    new Record(Map.of("key", 2L)),
                    new Record(Map.of("key", 7L))));

            // When
            List<Record> records = getRecords(queryAllRecords());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of("key", 7L)));
        }

        @Test
        void shouldFailIfAFileDoesNotExist() {
            addFileMetadata(fileReferenceFactory().rootFile("file.parquet", 10L));

            // When / Then
            assertThatThrownBy(() -> getRecords(queryAllRecords()))
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(QueryException.class)
                    .cause().isInstanceOf(RecordRetrievalException.class)
                    .cause().isInstanceOf(NoSuchElementException.class)
                    .hasNoCause();
        }
    }

    @Nested
    @DisplayName("Request value fields")
    class RequestValueFields {

        private final Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(
                        new Field("A", new StringType()),
                        new Field("B", new LongType()),
                        new Field("C", new ByteArrayType()))
                .build();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        void shouldReturnAllFieldsWhenNotRequestingValueFields() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(
                    new Record(Map.of(
                            "key", 1L,
                            "A", "first",
                            "B", 11L,
                            "C", new byte[]{1, 1}))));

            // When
            List<Record> records = getRecords(queryAllRecords());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of(
                            "key", 1L,
                            "A", "first",
                            "B", 11L,
                            "C", new byte[]{1, 1})));
        }

        @Test
        void shouldExcludeFieldsWhenRequestingValueFields() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(
                    new Record(Map.of(
                            "key", 1L,
                            "A", "first",
                            "B", 11L,
                            "C", new byte[]{1, 1}))));

            // When
            List<Record> records = getRecords(queryAllRecordsBuilder()
                    .processingConfig(requestValueFields("A"))
                    .build());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of("key", 1L, "A", "first")));
        }
    }

    @Nested
    @DisplayName("Apply iterators")
    class ApplyIterators {

        private final Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            addRootFile("file.parquet", List.of(
                    new Record(Map.of("key", "A", "value", 2L)),
                    new Record(Map.of("key", "A", "value", 2L)),
                    new Record(Map.of("key", "B", "value", 3L)),
                    new Record(Map.of("key", "B", "value", 4L))));
        }

        @Test
        void shouldApplyTableIterator() throws Exception {
            // Given
            tableProperties.set(ITERATOR_CLASS_NAME, AdditionIterator.class.getName());

            // When
            List<Record> records = getRecords(queryAllRecords());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of("key", "A", "value", 4L)),
                    new Record(Map.of("key", "B", "value", 7L)));
        }

        @Test
        void shouldApplyQueryIterator() throws Exception {
            // When
            List<Record> records = getRecords(queryAllRecordsBuilder()
                    .processingConfig(applyIterator(AdditionIterator.class))
                    .build());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of("key", "A", "value", 4L)),
                    new Record(Map.of("key", "B", "value", 7L)));
        }

        @Test
        void shouldApplyQueryIteratorWithConfig() throws Exception {
            // When
            List<Record> records = getRecords(queryAllRecordsBuilder()
                    .processingConfig(applyIterator(SecurityFilteringIterator.class, "key,B"))
                    .build());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of("key", "B", "value", 3L)),
                    new Record(Map.of("key", "B", "value", 4L)));
        }

        @Test
        void shouldApplyTableIteratorThenQueryIterator() throws Exception {
            // Given
            tableProperties.set(ITERATOR_CLASS_NAME, SecurityFilteringIterator.class.getName());
            tableProperties.set(ITERATOR_CONFIG, "key,B");

            // When
            List<Record> records = getRecords(queryAllRecordsBuilder()
                    .processingConfig(applyIterator(AdditionIterator.class))
                    .build());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of("key", "B", "value", 7L)));
        }
    }

    @Nested
    @DisplayName("Reinitialise based on a timeout")
    class ReinitialiseOnTimeout {

        @Test
        public void shouldReloadActiveFilesFromStateStoreWhenTimedOut() throws Exception {
            // Given files are added after the executor is first initialised
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
            QueryExecutor queryExecutor = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When the first initialisation has expired
            queryExecutor.initIfNeeded(Instant.parse("2023-11-27T09:35:00Z"));

            // Then the records that were added are found
            assertThat(getRecords(queryExecutor, queryAllRecords()))
                    .containsExactly(new Record(Map.of("key", 123L)));
        }

        @Test
        public void shouldNotReloadActiveFilesBeforeTimeOut() throws Exception {
            // Given files are added after the executor is first initialised
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
            QueryExecutor queryExecutor = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When the first initialisation has not yet expired
            queryExecutor.initIfNeeded(Instant.parse("2023-11-27T09:31:00Z"));

            // Then the records that were added are not found
            assertThat(getRecords(queryExecutor, queryAllRecords())).isEmpty();
        }
    }

    private void addRootFile(String filename, List<Record> records) {
        addFile(fileReferenceFactory().rootFile(filename, records.size()), records);
    }

    private void addPartitionFile(String partitionId, String filename, List<Record> records) {
        addFile(fileReferenceFactory().partitionFile(partitionId, filename, records.size()), records);
    }

    private void addFile(FileReference fileReference, List<Record> records) {
        addFileMetadata(fileReference);
        recordStore.addFile(fileReference.getFilename(), records);
    }

    private void addFileMetadata(FileReference fileReference) {
        stateStore.addFile(fileReference);
    }

    private QueryExecutor executor() throws Exception {
        return executorAtTime(Instant.now());
    }

    private QueryExecutor executorAtTime(Instant time) throws Exception {
        QueryExecutor executor = uninitialisedExecutorAtTime(time);
        executor.init(time);
        return executor;
    }

    private QueryExecutor uninitialisedExecutorAtTime(Instant time) {
        return new QueryExecutor(ObjectFactory.noUserJars(), stateStore, tableProperties, recordStore, time);
    }

    private List<Record> getRecords(Query query) throws Exception {
        return getRecords(executor(), query);
    }

    private List<Record> getRecords(QueryExecutor executor, Query query) {
        try (var it = executor.execute(query)) {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, IMMUTABLE), false)
                    .collect(Collectors.toUnmodifiableList());
        } catch (QueryException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Query.Builder query() {
        return Query.builder().queryId(UUID.randomUUID().toString())
                .tableName(tableProperties.get(TABLE_NAME));
    }

    private Query queryAllRecords() {
        return queryAllRecordsBuilder().build();
    }

    private Query.Builder queryAllRecordsBuilder() {
        return query()
                .regions(List.of(partitionTree().getRootPartition().getRegion()));
    }

    private Query queryRange(Object min, Object max) {
        return queryRegions(range(min, max));
    }

    private Query queryRegions(Region... regions) {
        return query()
                .regions(List.of(regions))
                .build();
    }

    private Region range(Object min, Object max) {
        Field field = tableProperties.getSchema().getField("key").orElseThrow();
        return new Region(new Range(field, min, max));
    }

    private PartitionTree partitionTree() {
        return new PartitionTree(stateStore.getAllPartitions());
    }

    private FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(stateStore);
    }

    private static QueryProcessingConfig requestValueFields(String... fields) {
        return QueryProcessingConfig.builder()
                .requestedValueFields(List.of(fields))
                .build();
    }

    private static QueryProcessingConfig applyIterator(Class<?> iteratorClass) {
        return QueryProcessingConfig.builder()
                .queryTimeIteratorClassName(iteratorClass.getName())
                .build();
    }

    private static QueryProcessingConfig applyIterator(Class<?> iteratorClass, String config) {
        return QueryProcessingConfig.builder()
                .queryTimeIteratorClassName(iteratorClass.getName())
                .queryTimeIteratorConfig(config)
                .build();
    }
}
