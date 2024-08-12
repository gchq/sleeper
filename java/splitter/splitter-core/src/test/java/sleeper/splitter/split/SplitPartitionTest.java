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
package sleeper.splitter.split;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.sketches.Sketches;
import sleeper.splitter.split.FindPartitionSplitPoint.SketchesLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_ASYNC_COMMIT;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithPartitions;

public class SplitPartitionTest {
    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder().rowKeyFields(field).build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    private final Map<String, Sketches> fileToSketchMap = new HashMap<>();
    private final List<SplitPartitionCommitRequest> sentAsyncCommits = new ArrayList<>();

    @Nested
    @DisplayName("Skip split")
    class SkipSplit {
        @Test
        public void shouldNotSplitPartitionForIntKeyIfItCannotBeSplitBecausePartitionIsOnePoint() throws Exception {
            // Given
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id12", "id3", 1)
                    .splitToNewChildren("id12", "id1", "id2", 0)
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(tree.getAllPartitions());

            for (Partition partition : tree.getAllPartitions()) {
                int minRange = (int) partition.getRegion().getRange("key").getMin();
                int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
                for (int i = 0; i < 10; i++) {
                    List<Record> records = new ArrayList<>();
                    int j = 0;
                    for (int r = minRange; r < maxRange && j < 10; r++, j++) {
                        Record record = new Record();
                        record.put("key", r);
                        records.add(record);
                    }
                    ingestRecordsToSketchOnPartition(schema, stateStore, partition.getId(), records.stream());
                }
            }

            // When
            splitPartition(schema, stateStore, "id2", generateNoIds());

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldNotSplitPartitionForIntKeyIfItCannotBeSplitBecauseDataIsConstant() throws Exception {
            // Given
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id12", "id3", 10)
                    .splitToNewChildren("id12", "id1", "id2", 0)
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(tree.getAllPartitions());

            for (Partition partition : stateStore.getAllPartitions()) {
                for (int i = 0; i < 10; i++) {
                    int minRange = (int) partition.getRegion().getRange("key").getMin();
                    int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
                    List<Record> records = new ArrayList<>();
                    int j = 0;
                    if (!partition.getId().equals("id2")) {
                        for (int r = minRange; r < maxRange && j < 10; r++, j++) {
                            Record record = new Record();
                            record.put("key", r);
                            records.add(record);
                        }
                    } else {
                        // Files in partition2 all have the same value for the key
                        for (int r = 0; r < 10; r++) {
                            Record record = new Record();
                            record.put("key", 1);
                            records.add(record);
                        }
                    }
                    ingestRecordsToSketchOnPartition(schema, stateStore, partition.getId(), records.stream());
                }
            }

            // When
            splitPartition(schema, stateStore, "id2", generateNoIds());

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldNotSplitPartitionForByteArrayKeyIfItCannotBeSplitBecausePartitionIsOnePoint() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new ByteArrayType())).build();
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id12", "id3", new byte[]{(byte) 51})
                    .splitToNewChildren("id12", "id1", "id2", new byte[]{(byte) 50})
                    .buildTree();

            StateStore stateStore = inMemoryStateStoreWithPartitions(tree.getAllPartitions());

            for (Partition partition : tree.getAllPartitions()) {
                for (int i = 0; i < 10; i++) {
                    List<Record> records = new ArrayList<>();
                    if (partition.getId().equals("id1")) {
                        int j = 0;
                        for (byte r = (byte) 0;
                             r < ((byte[]) partition.getRegion().getRange("key").getMax())[0] && j < 10;
                             r++, j++) {
                            Record record = new Record();
                            record.put("key", new byte[]{r});
                            records.add(record);
                        }
                    } else if (partition.getId().equals("id2")) {
                        for (int j = 0; j < 10; j++) {
                            Record record = new Record();
                            record.put("key", new byte[]{50});
                            records.add(record);
                        }
                    } else {
                        for (int j = 51; j < 60; j++) {
                            Record record = new Record();
                            record.put("key", new byte[]{(byte) j});
                            records.add(record);
                        }
                    }
                    ingestRecordsToSketchOnPartition(schema, stateStore, partition.getId(), records.stream());
                }
            }

            // When
            splitPartition(schema, stateStore, "id2", generateNoIds());

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldNotSplitPartitionForByteArrayKeyIfItCannotBeSplitBecauseDataIsConstant() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new ByteArrayType())).build();
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id12", "id3", new byte[]{(byte) 100})
                    .splitToNewChildren("id12", "id1", "id2", new byte[]{(byte) 50})
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(tree.getAllPartitions());

            for (Partition partition : stateStore.getAllPartitions()) {
                for (int i = 0; i < 10; i++) {
                    List<Record> records = new ArrayList<>();
                    if (partition.getId().equals("id1")) {
                        int j = 0;
                        for (byte r = (byte) 0;
                             r < ((byte[]) partition.getRegion().getRange("key").getMax())[0] && j < 10;
                             r++, j++) {
                            Record record = new Record();
                            record.put("key", new byte[]{r});
                            records.add(record);
                        }
                    } else if (partition.getId().equals("id2")) {
                        // Files in partition2 all have the same value for the key
                        for (int j = 0; j < 10; j++) {
                            Record record = new Record();
                            record.put("key", new byte[]{60});
                            records.add(record);
                        }
                    } else {
                        for (int j = 100; j < 110; j++) {
                            Record record = new Record();
                            record.put("key", new byte[]{(byte) j});
                            records.add(record);
                        }
                    }
                    ingestRecordsToSketchOnPartition(schema, stateStore, partition.getId(), records.stream());
                }
            }

            // When
            splitPartition(schema, stateStore, "id2", generateNoIds());

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }
    }

    @Nested
    @DisplayName("Single dimension split")
    class SingleDimensionSplit {
        @Test
        void shouldSplitPartitionForIntKey() throws Exception {
            // Given
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(100 * i, 100 * (i + 1))
                                    .mapToObj(r -> new Record(Map.of("key", r)))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildren("A", "B", "C", 500)
                            .buildList());
        }

        @Test
        void shouldSplitPartitionForLongKey() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new LongType())).build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            LongStream.range(100L * i, 100L * (i + 1))
                                    .mapToObj(r -> new Record(Map.of("key", r)))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildren("A", "B", "C", 500L)
                            .buildList());
        }

        @Test
        void shouldSplitPartitionForStringKey() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    .mapToObj(r -> new Record(Map.of("key", String.format("A%s%s", i, r))))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildren("A", "B", "C", "A50")
                            .buildList());
        }

        @Test
        void shouldSplitPartitionForByteArrayKey() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new ByteArrayType())).build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    .mapToObj(r -> new Record(Map.of("key", new byte[]{(byte) r})))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildren("A", "B", "C", new byte[]{(byte) 50})
                            .buildList());
        }
    }

    @Nested
    @DisplayName("Multidimensional split")
    class MultidimensionalSplit {
        @Test
        public void shouldSplitIntKeyOnFirstDimension() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    .mapToObj(r -> new Record(Map.of(
                                            "key1", r,
                                            "key2", 10)))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildrenOnDimension("A", "B", "C", 0, 50)
                            .buildList());
        }

        @Test
        public void shouldSplitIntKeyOnSecondDimensionWhenAllValuesForFirstKeyAreTheSame() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    .mapToObj(r -> new Record(Map.of(
                                            "key1", 10,
                                            "key2", r)))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildrenOnDimension("A", "B", "C", 1, 50)
                            .buildList());
        }

        @Test
        public void shouldSplitIntKeyOnFirstDimensionWhenSecondDimensionCanBeSplit() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    .mapToObj(r -> new Record(Map.of(
                                            "key1", r,
                                            "key2", i)))));

            // When
            splitSinglePartition(schema, stateStore, generateIdsStartingFrom('B'));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildrenOnDimension("A", "B", "C", 0, 50)
                            .buildList());
        }

        @Test
        public void shouldSplitIntKeyOnSecondDimensionWhenMinAndMedianForFirstKeyAreTheSame() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    // The majority of the values are 10; so min should equal median
                                    .mapToObj(r -> new Record(Map.of(
                                            "key1", r < 75 ? 10 : 20,
                                            "key2", r)))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildrenOnDimension("A", "B", "C", 1, 50)
                            .buildList());
        }

        @Test
        public void shouldSplitByteKeyOnFirstDimension() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());

            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    .mapToObj(r -> new Record(Map.of(
                                            "key1", new byte[]{(byte) r},
                                            "key2", new byte[]{(byte) -100})))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildrenOnDimension("A", "B", "C", 0, new byte[]{50})
                            .buildList());
        }

        @Test
        public void shouldSplitByteKeyOnSecondDimensionWhenAllValuesForFirstKeyAreTheSame() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            IntStream.range(0, 10)
                    .forEach(i -> ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                            IntStream.range(0, 100)
                                    .mapToObj(r -> new Record(Map.of(
                                            "key1", new byte[]{(byte) -100},
                                            "key2", new byte[]{(byte) r})))));

            // When
            splitSinglePartition(schema, stateStore, generateIds("B", "C"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildrenOnDimension("A", "B", "C", 1, new byte[]{50})
                            .buildList());
        }
    }

    @Nested
    @DisplayName("Commit partition split asynchronously")
    class AsynchronousCommit {

        @Test
        @Disabled("TODO")
        void shouldCommitPartitionSplitAsynchronously() throws Exception {
            // Given
            PartitionTree tree = new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(tree.getAllPartitions());
            String filename = ingestRecordsToSketchOnPartition(schema, stateStore, "A",
                    IntStream.rangeClosed(1, 100)
                            .mapToObj(i -> new Record(Map.of("key", i))));
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
            tableProperties.set(PARTITION_SPLIT_ASYNC_COMMIT, "true");

            // When
            partitionSplitter(stateStore, tableProperties, generateIds("B", "C"))
                    .splitPartition(tree.getRootPartition(), List.of(filename));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }
    }

    private String ingestRecordsToSketchOnPartition(Schema schema, StateStore stateStore, String partitionId, Stream<Record> recordsStream) {
        Sketches sketches = Sketches.from(schema);
        AtomicLong recordCount = new AtomicLong();

        recordsStream.forEach(rec -> {
            sketches.update(schema, rec);
            recordCount.incrementAndGet();
        });

        FileReference recordFileReference = FileReferenceFactory.from(stateStore).partitionFile(partitionId, UUID.randomUUID().toString(), recordCount.get());
        try {
            stateStore.addFile(recordFileReference);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        fileToSketchMap.put(recordFileReference.getFilename(), sketches);
        return recordFileReference.getFilename();
    }

    private void splitSinglePartition(Schema schema, StateStore stateStore, Supplier<String> generateIds) throws Exception {
        Partition partition = stateStore.getAllPartitions().get(0);
        List<String> fileNames = stateStore.getFileReferences().stream()
                .map(FileReference::getFilename)
                .collect(Collectors.toList());
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        SplitPartition partitionSplitter = partitionSplitter(stateStore, tableProperties, generateIds);
        partitionSplitter.splitPartition(partition, fileNames);
    }

    private void splitPartition(Schema schema, StateStore stateStore, String partitionId, Supplier<String> generateIds) throws Exception {
        PartitionTree tree = new PartitionTree(stateStore.getAllPartitions());
        Partition partition = tree.getPartition(partitionId);
        List<String> fileNames = stateStore.getFileReferences().stream()
                .filter(file -> partitionId.equals(file.getPartitionId()))
                .map(FileReference::getFilename)
                .collect(Collectors.toList());
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        SplitPartition partitionSplitter = partitionSplitter(stateStore, tableProperties, generateIds);
        partitionSplitter.splitPartition(partition, fileNames);
    }

    private SplitPartition partitionSplitter(StateStore stateStore, TableProperties tableProperties, Supplier<String> generateIds) {
        return new SplitPartition(stateStore, tableProperties, loadSketchesFromMap(), generateIds, sentAsyncCommits::add);
    }

    public SketchesLoader loadSketchesFromMap() {
        return (filename) -> fileToSketchMap.get(filename);
    }

    private static Supplier<String> generateIds(String... ids) {
        return Arrays.stream(ids).iterator()::next;
    }

    private static Supplier<String> generateIdsStartingFrom(char start) {
        return Stream.iterate(start, c -> (char) (c + 1)).map(String::valueOf).iterator()::next;
    }

    private static Supplier<String> generateNoIds() {
        return () -> {
            throw new IllegalArgumentException("Generated an ID, expected none");
        };
    }
}
