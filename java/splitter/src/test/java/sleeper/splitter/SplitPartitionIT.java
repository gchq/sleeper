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
package sleeper.splitter;

import com.facebook.collections.ByteArray;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.testutils.IngestCoordinatorTestHelper;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithPartitions;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class SplitPartitionIT {
    @TempDir
    public Path folder;

    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder().rowKeyFields(field).build();

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

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();

            for (Partition partition : tree.getAllPartitions()) {
                for (int i = 0; i < 10; i++) {
                    List<Record> records = new ArrayList<>();
                    int j = 0;
                    int minRange = (int) partition.getRegion().getRange("key").getMin();
                    int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
                    for (int r = minRange; r < maxRange && j < 10; r++, j++) {
                        Record record = new Record();
                        record.put("key", r);
                        records.add(record);
                    }
                    ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
                }
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());
            Partition partition2 = tree.getPartition("id2");

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(partition2, fileNames);

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("root"), tree.getPartition("id12"),
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
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

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();

            for (Partition partition : stateStore.getAllPartitions()) {
                for (int i = 0; i < 10; i++) {
                    List<Record> records = new ArrayList<>();
                    int j = 0;
                    if (!partition.getId().equals("id2")) {
                        int minRange = (int) partition.getRegion().getRange("key").getMin();
                        int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
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
                    ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
                }
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals("id2"))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(tree.getPartition("id2"), fileNames);

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("root"), tree.getPartition("id12"),
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
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

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            for (Partition partition : tree.getAllPartitions()) {
                for (int i = 0; i < 10; i++) {
                    List<Record> records = new ArrayList<>();
                    for (int r = 0; r < 100; r++) {
                        Record record = new Record();
                        record.put("key", new byte[]{(byte) r});
                        records.add(record);
                    }
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
                    ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
                }
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals("id2"))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(tree.getPartition("id2"), fileNames);

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("root"), tree.getPartition("id12"),
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
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

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
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
                    ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
                }
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals("id2"))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(tree.getPartition("id2"), fileNames);

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("root"), tree.getPartition("id12"),
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("id1"), tree.getPartition("id2"), tree.getPartition("id3"));
        }
    }

    @Nested
    @DisplayName("Single dimension split")
    class SingleDimensionSplit {
        @Test
        void shouldSplitPartitionForIntKey() throws Exception {
            // Given
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .singlePartition("A").buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", 500)
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(treeBefore.getAllPartitions());

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 100 * i; r < 100 * (i + 1); r++) {
                    Record record = new Record();
                    record.put("key", r);
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            Supplier<String> idSupplier = List.of("B", "C").iterator()::next;
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration(), idSupplier);

            // When
            partitionSplitter.splitPartition(treeBefore.getRootPartition(),
                    stateStore.getActiveFiles().stream().map(FileInfo::getFilename).collect(Collectors.toList()));

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("A"), treeAfter.getPartition("B"), treeAfter.getPartition("C"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("B"), treeAfter.getPartition("C"));
        }

        @Test
        void shouldSplitPartitionForLongKey() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new LongType())).build();
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .singlePartition("A").buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", 500L)
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(treeBefore.getAllPartitions());

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (long r = 100L * i; r < 100L * (i + 1); r++) {
                    Record record = new Record();
                    record.put("key", r);
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            Supplier<String> idSupplier = List.of("B", "C").iterator()::next;
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration(), idSupplier);

            // When
            partitionSplitter.splitPartition(treeBefore.getRootPartition(),
                    stateStore.getActiveFiles().stream().map(FileInfo::getFilename).collect(Collectors.toList()));

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("A"), treeAfter.getPartition("B"), treeAfter.getPartition("C"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("B"), treeAfter.getPartition("C"));
        }

        @Test
        void shouldSplitPartitionForStringKey() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .singlePartition("A").buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "A50")
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(treeBefore.getAllPartitions());

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    //A + value from 0-10100
                    record.put("key", "A" + i + "" + r);
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            Supplier<String> idSupplier = List.of("B", "C").iterator()::next;
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration(), idSupplier);

            // When
            partitionSplitter.splitPartition(treeBefore.getRootPartition(),
                    stateStore.getActiveFiles().stream().map(FileInfo::getFilename).collect(Collectors.toList()));

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("A"), treeAfter.getPartition("B"), treeAfter.getPartition("C"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("B"), treeAfter.getPartition("C"));
        }

        @Test
        void shouldSplitPartitionForByteArrayKey() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new ByteArrayType())).build();
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .singlePartition("A").buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", new byte[]{(byte) 50})
                    .buildTree();
            StateStore stateStore = inMemoryStateStoreWithPartitions(treeBefore.getAllPartitions());

            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    record.put("key", new byte[]{(byte) r});
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            Supplier<String> idSupplier = List.of("B", "C").iterator()::next;
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration(), idSupplier);

            // When
            partitionSplitter.splitPartition(treeBefore.getRootPartition(),
                    stateStore.getActiveFiles().stream().map(FileInfo::getFilename).collect(Collectors.toList()));

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("A"), treeAfter.getPartition("B"), treeAfter.getPartition("C"));
            assertThat(stateStore.getLeafPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("B"), treeAfter.getPartition("C"));
        }
    }

    @Nested
    @DisplayName("Multidimensional split")
    class MultidimensionalSplit {
        @Test
        public void shouldSplitPartitionForIntMultidimensionalKeyOnFirstDimensionCorrectly() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema)
                    .singlePartition("A")
                    .buildList());
            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            Partition rootPartition = stateStore.getAllPartitions().get(0);
            ingestRecordsFromIterator(stateStore, schema, path, path2,
                    IntStream.range(0, 1000)
                            .mapToObj(i -> {
                                Record record = new Record();
                                record.put("key1", i % 100);
                                record.put("key2", 10);
                                return record;
                            }).iterator());
            Supplier<String> idSupplier = List.of("B", "C").iterator()::next;
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration(), idSupplier);

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(rootPartition, fileNames);

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("A")
                            .splitToNewChildrenOnDimension("A", "B", "C", 0, 50)
                            .buildList());
        }

        @Test
        public void shouldSplitPartitionForIntMultidimensionalKeyOnSecondDimensionCorrectlyWhenMinIsMax() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            Partition rootPartition = stateStore.getAllPartitions().get(0);
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    record.put("key1", 10);
                    record.put("key2", r);
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(rootPartition, fileNames);

            // Then
            //  - There should be 3 partitions
            List<Partition> partitions = stateStore.getAllPartitions();
            assertThat(partitions).hasSize(3);
            //  - There should be 1 non-leaf partition
            List<Partition> nonLeafPartitions = partitions.stream()
                    .filter(p -> !p.isLeafPartition())
                    .collect(Collectors.toList());
            //  - The root partition should have been split on the second dimension
            assertThat(nonLeafPartitions).hasSize(1)
                    .extracting(Partition::getDimension)
                    .containsExactly(1);
            //  - The leaf partitions should have been split on a value which is between
            //      0 and 100.
            Set<Partition> leafPartitions = partitions.stream()
                    .filter(Partition::isLeafPartition)
                    .collect(Collectors.toSet());
            Iterator<Partition> it = leafPartitions.iterator();
            Object splitPoint = splitPoint(it.next(), it.next(), "key2");
            assertThat(leafPartitions)
                    .extracting(partition -> partition.getRegion().getRange("key2"))
                    .extracting(Range::getMin, Range::getMax)
                    .containsExactlyInAnyOrder(
                            tuple(Integer.MIN_VALUE, splitPoint),
                            tuple(splitPoint, null));
            assertThat((int) splitPoint).isStrictlyBetween(Integer.MIN_VALUE, 99);
            //  - The leaf partitions should have the root partition as their parent
            //      and an empty array for the child partitions.
            assertThat(leafPartitions).allSatisfy(partition -> {
                assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
                assertThat(partition.getChildPartitionIds()).isEmpty();
            });
            assertThat(nonLeafPartitions).containsExactly(rootPartition);
        }

        @Test
        public void shouldSplitPartitionForIntMultidimensionalKeyOnSecondDimensionCorrectlyWhenMinIsMedian() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            Partition rootPartition = stateStore.getAllPartitions().get(0);
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    // The majority of the values are 10; so min should equal median
                    if (r < 75) {
                        record.put("key1", 10);
                    } else {
                        record.put("key1", 20);
                    }
                    record.put("key2", r);
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(rootPartition, fileNames);

            // Then
            //  - There should be 3 partitions
            List<Partition> partitions = stateStore.getAllPartitions();
            assertThat(partitions).hasSize(3);
            //  - There should be 1 non-leaf partition
            List<Partition> nonLeafPartitions = partitions.stream()
                    .filter(p -> !p.isLeafPartition())
                    .collect(Collectors.toList());
            //  - The root partition should have been split on the second dimension
            assertThat(nonLeafPartitions).hasSize(1)
                    .extracting(Partition::getDimension)
                    .containsExactly(1);
            //  - The leaf partitions should have been split on a value which is between
            //      0 and 100.
            Set<Partition> leafPartitions = partitions.stream()
                    .filter(Partition::isLeafPartition)
                    .collect(Collectors.toSet());
            Iterator<Partition> it = leafPartitions.iterator();
            Object splitPoint = splitPoint(it.next(), it.next(), "key2");
            assertThat(leafPartitions)
                    .extracting(partition -> partition.getRegion().getRange("key2"))
                    .extracting(Range::getMin, Range::getMax)
                    .containsExactlyInAnyOrder(
                            tuple(Integer.MIN_VALUE, splitPoint),
                            tuple(splitPoint, null));
            assertThat((int) splitPoint).isStrictlyBetween(Integer.MIN_VALUE, 99);
            //  - The leaf partitions should have the root partition as their parent
            //      and an empty array for the child partitions.
            assertThat(leafPartitions).allSatisfy(partition -> {
                assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
                assertThat(partition.getChildPartitionIds()).isEmpty();
            });
            assertThat(nonLeafPartitions).containsExactly(rootPartition);
        }


        @Test
        public void shouldSplitPartitionForByteArrayMultidimensionalKeyOnFirstDimensionCorrectly() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            Partition rootPartition = stateStore.getAllPartitions().get(0);
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    record.put("key1", new byte[]{(byte) r});
                    record.put("key2", new byte[]{(byte) -100});
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(rootPartition, fileNames);

            // Then
            //  - There should be 3 partitions
            List<Partition> partitions = stateStore.getAllPartitions();
            assertThat(partitions).hasSize(3);
            //  - There should be 1 non-leaf partition
            List<Partition> nonLeafPartitions = partitions.stream()
                    .filter(p -> !p.isLeafPartition())
                    .collect(Collectors.toList());
            //  - The root partition should have been split on the first dimension
            assertThat(nonLeafPartitions).hasSize(1)
                    .extracting(Partition::getDimension)
                    .containsExactly(0);
            //  - The leaf partitions should have been split on a value which is between
            //      0 and 100.
            Set<Partition> leafPartitions = partitions.stream()
                    .filter(Partition::isLeafPartition)
                    .collect(Collectors.toSet());
            Iterator<Partition> it = leafPartitions.iterator();
            byte[] splitPoint = splitPointBytes(it.next(), it.next(), "key1");
            assertThat(leafPartitions)
                    .extracting(partition -> partition.getRegion().getRange("key1"))
                    .extracting(Range::getMin, Range::getMax)
                    .containsExactlyInAnyOrder(
                            tuple(new byte[]{}, splitPoint),
                            tuple(splitPoint, null));
            assertThat(ByteArray.wrap(splitPoint)).isStrictlyBetween(
                    ByteArray.wrap(new byte[]{}),
                    ByteArray.wrap(new byte[]{99}));
            //  - The leaf partitions should have the root partition as their parent
            //      and an empty array for the child partitions.
            assertThat(leafPartitions).allSatisfy(partition -> {
                assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
                assertThat(partition.getChildPartitionIds()).isEmpty();
            });
            assertThat(nonLeafPartitions).containsExactly(rootPartition);
        }

        @Test
        public void shouldSplitPartitionForByteArrayMultidimensionalKeyOnSecondDimensionCorrectly() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()))
                    .build();
            StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
            String path = createTempDirectory(folder, null).toString();
            String path2 = createTempDirectory(folder, null).toString();
            Partition rootPartition = stateStore.getAllPartitions().get(0);
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    record.put("key1", new byte[]{(byte) -100});
                    record.put("key2", new byte[]{(byte) r});
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
            SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

            // When
            List<String> fileNames = stateStore.getActiveFiles().stream()
                    .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                    .map(FileInfo::getFilename)
                    .collect(Collectors.toList());
            partitionSplitter.splitPartition(rootPartition, fileNames);

            // Then
            List<Partition> partitions = stateStore.getAllPartitions();
            assertThat(partitions).hasSize(3);
            List<Partition> nonLeafPartitions = partitions.stream()
                    .filter(p -> !p.isLeafPartition())
                    .collect(Collectors.toList());
            assertThat(nonLeafPartitions).hasSize(1)
                    .extracting(Partition::getDimension)
                    .containsExactly(1);
            Set<Partition> leafPartitions = partitions.stream()
                    .filter(Partition::isLeafPartition)
                    .collect(Collectors.toSet());
            Iterator<Partition> it = leafPartitions.iterator();
            byte[] splitPoint = splitPointBytes(it.next(), it.next(), "key2");
            assertThat(leafPartitions)
                    .extracting(partition -> partition.getRegion().getRange("key2"))
                    .extracting(Range::getMin, Range::getMax)
                    .containsExactlyInAnyOrder(
                            tuple(new byte[]{}, splitPoint),
                            tuple(splitPoint, null));
            assertThat(ByteArray.wrap(splitPoint)).isStrictlyBetween(
                    ByteArray.wrap(new byte[]{}),
                    ByteArray.wrap(new byte[]{99}));
            assertThat(leafPartitions).allSatisfy(partition -> {
                assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
                assertThat(partition.getChildPartitionIds()).isEmpty();
            });
            assertThat(nonLeafPartitions).containsExactly(rootPartition);
        }
    }

    private static byte[] splitPointBytes(Partition partition1, Partition partition2, String key) {
        Range range1 = partition1.getRegion().getRange(key);
        Range range2 = partition2.getRegion().getRange(key);
        if (Arrays.equals((byte[]) range1.getMin(), (byte[]) range2.getMax())) {
            return (byte[]) range1.getMin();
        } else {
            return (byte[]) range1.getMax();
        }
    }

    private static Object splitPoint(Partition partition1, Partition partition2, String key) {
        Range range1 = partition1.getRegion().getRange(key);
        Range range2 = partition2.getRegion().getRange(key);
        if (Objects.equals(range1.getMin(), range2.getMax())) {
            return range1.getMin();
        } else {
            return range1.getMax();
        }
    }

    private static void ingestRecordsFromIterator(StateStore stateStore, Schema schema, String localDir,
                                                  String filePathPrefix, Iterator<Record> recordIterator) throws Exception {
        ParquetConfiguration parquetConfiguration = IngestCoordinatorTestHelper.parquetConfiguration(schema, new Configuration());
        IngestCoordinator<Record> ingestCoordinator = IngestCoordinatorTestHelper.standardIngestCoordinator(
                stateStore, schema,
                ArrayListRecordBatchFactory.builder()
                        .localWorkingDirectory(localDir)
                        .maxNoOfRecordsInMemory(1000000)
                        .maxNoOfRecordsInLocalStore(100000000)
                        .parquetConfiguration(parquetConfiguration)
                        .buildAcceptingRecords(),
                DirectPartitionFileWriterFactory.from(parquetConfiguration, filePathPrefix));
        new IngestRecordsFromIterator(ingestCoordinator, recordIterator).write();
    }

    private static void splitSinglePartition(Schema schema, StateStore stateStore) throws Exception {
        Partition partition = stateStore.getAllPartitions().get(0);
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());
        partitionSplitter.splitPartition(partition, fileNames);
    }
}
