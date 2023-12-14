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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.SplitFileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.dynamodb.tools.DynamoDBContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FilesReportTestHelper.readyForGCFileReport;
import static sleeper.core.statestore.FilesReportTestHelper.splitFileReport;
import static sleeper.core.statestore.FilesReportTestHelper.wholeFilesReport;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DynamoDBStateStoreIT {
    private static AmazonDynamoDB dynamoDBClient;

    @Container
    public static DynamoDBContainer dynamoDb = new DynamoDBContainer();

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, dynamoDb.getDynamoPort(), AmazonDynamoDBClientBuilder.standard());
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    @BeforeEach
    void setUp() {
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
    }

    private TableProperties createTable(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, garbageCollectorDelayBeforeDeletionInMinutes);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBStateStore.class.getName());
        return tableProperties;
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions,
                                             int garbageCollectorDelayBeforeDeletionInMinutes) {
        TableProperties tableProperties = createTable(schema, garbageCollectorDelayBeforeDeletionInMinutes);
        DynamoDBStateStore stateStore = new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDBClient);
        try {
            stateStore.initialise(partitions);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return stateStore;
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions) {
        return getStateStore(schema, partitions, 0);
    }

    private DynamoDBStateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct(), garbageCollectorDelayBeforeDeletionInMinutes);
    }

    private DynamoDBStateStore getStateStore(Schema schema) {
        return getStateStore(schema, 0);
    }

    private Schema schemaWithSingleRowKeyType(PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field("key", type)).build();
    }

    private Schema schemaWithTwoRowKeyTypes(PrimitiveType type1, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(new Field("key1", type1), new Field("key2", type2)).build();
    }

    private Schema schemaWithKeyAndValueWithTypes(PrimitiveType keyType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", keyType))
                .valueFields(new Field("value", valueType))
                .build();
    }

    @Nested
    @DisplayName("Active files")
    class ActiveFiles {
        @Test
        public void shouldReturnCorrectFileInfoForLongRowKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("abc")
                    .numberOfRecords(100L)
                    .partitionId("1")
                    .build();
            dynamoDBStateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            dynamoDBStateStore.addFile(fileInfo);

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
            });
        }

        @Test
        public void shouldReturnCorrectFileInfoForByteArrayKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("abc")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            dynamoDBStateStore.addFile(fileInfo);

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
            });
        }

        @Test
        public void shouldReturnCorrectFileInfoFor2DimensionalByteArrayKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("abc")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            dynamoDBStateStore.addFile(fileInfo);

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
            });
        }

        @Test
        public void shouldReturnCorrectFileInfoForMultidimensionalRowKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithTwoRowKeyTypes(new LongType(), new StringType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("abc")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            dynamoDBStateStore.addFile(fileInfo);

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
            });
        }

        @Test
        public void shouldReturnAllFileInfos() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            dynamoDBStateStore.fixTime(Instant.ofEpochMilli(1_000_000L));
            FileInfoFactory fileInfoFactory = FileInfoFactory.from(schema, dynamoDBStateStore);
            Set<FileInfo> expected = new HashSet<>();
            for (int i = 0; i < 11; i++) {
                FileInfo fileInfo = fileInfoFactory.partitionFile("root", "file-" + i, 100L);
                dynamoDBStateStore.addFile(fileInfo);
                expected.add(fileInfo.toBuilder().lastStateStoreUpdateTime(1_000_000L).build());
            }

            // When
            List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();

            // Then
            assertThat(new HashSet<>(fileInfos)).isEqualTo(expected);
        }

        @Test
        void shouldStoreAndReturnPartialFile() throws Exception {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            dynamoDBStateStore.fixTime(Instant.ofEpochMilli(1_000_000L));
            FileInfo fileInfo = FileInfo.partialFile()
                    .filename("partial-file")
                    .partitionId("A")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.addFile(fileInfo);

            // When
            List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();

            // Then
            assertThat(fileInfos)
                    .containsExactly(fileInfo.toBuilder().lastStateStoreUpdateTime(1_000_000L).build());
        }

        @Test
        public void shouldReturnOnlyActiveFilesWithNoJobId() throws StateStoreException {
            // Given
            Schema schema = schemaWithKeyAndValueWithTypes(new LongType(), new StringType());
            DynamoDBStateStore dynamoDBStateStore = getStateStore(schema);
            FileInfo fileInfo1 = FileInfo.wholeFile()
                    .filename("file1")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.addFile(fileInfo1);
            FileInfo fileInfo2 = FileInfo.wholeFile()
                    .filename("file2")
                    .partitionId("2")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.addFile(fileInfo2);
            FileInfo fileInfo3 = FileInfo.wholeFile()
                    .filename("file3")
                    .partitionId("3")
                    .jobId("job1")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.addFile(fileInfo3);

            // When
            List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFilesWithNoJobId();

            // Then
            assertThat(fileInfos)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(fileInfo1, fileInfo2);
        }

        @Test
        void shouldReturnActiveFilesOrderedByPartitionIdThenFilename() throws Exception {
            // Given
            Schema schema = schemaWithKeyAndValueWithTypes(new LongType(), new StringType());
            DynamoDBStateStore dynamoDBStateStore = getStateStore(schema);
            FileInfo file1 = FileInfo.wholeFile()
                    .filename("file1")
                    .partitionId("P1")
                    .numberOfRecords(100L)
                    .build();
            FileInfo file2 = FileInfo.wholeFile()
                    .filename("file2")
                    .partitionId("P2")
                    .numberOfRecords(100L)
                    .build();
            FileInfo file3 = FileInfo.wholeFile()
                    .filename("file3")
                    .partitionId("P1")
                    .numberOfRecords(100L)
                    .build();
            FileInfo file4 = FileInfo.wholeFile()
                    .filename("file4")
                    .partitionId("P2")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.addFiles(List.of(file1, file2, file3, file4));

            // When/Then
            assertThat(dynamoDBStateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(file1, file3, file2, file4);
        }
    }

    @Nested
    @DisplayName("Ready for GC files")
    class ReadyForGCFiles {
        @Test
        public void shouldGetFilesThatAreReadyForGC() throws StateStoreException {
            // Given
            Instant file1Time = Instant.parse("2023-06-06T15:00:00Z");
            Instant file2Time = Instant.parse("2023-06-06T15:01:00Z");
            Instant file3Time = Instant.parse("2023-06-06T15:02:00Z");
            Schema schema = schemaWithKeyAndValueWithTypes(new IntType(), new StringType());
            PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
            DynamoDBStateStore stateStore = getStateStore(schema, tree.getAllPartitions(), 5);
            FileInfoFactory fileInfoFactory = FileInfoFactory.from(tree);
            //  - A file which should be garbage collected immediately
            FileInfo fileInfo1 = FileInfo.wholeFile()
                    .filename("file1")
                    .partitionId("root")
                    .numberOfRecords(100L)
                    .lastStateStoreUpdateTime(file1Time)
                    .build();
            stateStore.fixTime(file1Time);
            stateStore.addFile(fileInfo1);
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile("root", List.of("file1"),
                    fileInfoFactory.rootFile("compacted1", 100L));
            //  - An active file which should not be garbage collected
            FileInfo fileInfo2 = FileInfo.wholeFile()
                    .filename("file2")
                    .partitionId("root")
                    .numberOfRecords(100L)
                    .lastStateStoreUpdateTime(file2Time)
                    .build();
            stateStore.fixTime(file2Time);
            stateStore.addFile(fileInfo2);
            //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
            //      just been marked as ready for GC
            FileInfo fileInfo3 = FileInfo.wholeFile()
                    .filename("file3")
                    .partitionId("root")
                    .numberOfRecords(100L)
                    .lastStateStoreUpdateTime(file3Time)
                    .build();
            stateStore.fixTime(file3Time);
            stateStore.addFile(fileInfo3);
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile("root", List.of("file3"),
                    fileInfoFactory.rootFile("compacted3", 100L));

            // When / Then 1
            assertThat(stateStore.getReadyForGCFilenamesBefore(file1Time.plus(Duration.ofMinutes(1))))
                    .containsExactly("file1");

            // When / Then 2
            assertThat(stateStore.getReadyForGCFilenamesBefore(file3Time.plus(Duration.ofMinutes(1))))
                    .containsExactly("file1", "file3");
        }

        @Test
        public void shouldDeleteReadyForGCFilename() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            FileInfo fileInfo1 = FileInfo.wholeFile()
                    .filename("file1")
                    .partitionId("4")
                    .numberOfRecords(100L)
                    .build();
            dynamoDBStateStore.addFile(fileInfo1);
            FileInfo fileInfo2 = FileInfo.wholeFile()
                    .filename("file2")
                    .numberOfRecords(100L)
                    .partitionId("5")
                    .build();
            dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile("4", List.of("file1"), fileInfo2);

            // When
            dynamoDBStateStore.deleteReadyForGCFile("file1");

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(fileInfo2);
            assertThat(dynamoDBStateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
        }
    }

    @Nested
    @DisplayName("Report file status")
    class ReportFileStatus {
        Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
        Schema schema = schemaWithKey("key", new LongType());
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore store;
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(partitions.buildTree(), updateTime);

        @BeforeEach
        void setUp() {
            store = getStateStore(schema);
            store.fixTime(updateTime);
        }

        @Test
        void shouldReportOneActiveFile() throws Exception {
            // Given
            FileInfo file = factory.rootFile("test", 100L);
            store.addFile(file);

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(wholeFilesReport(file));
        }

        @Test
        void shouldReportOneReadyForGCFile() throws Exception {
            // Given
            FileInfo file = factory.rootFile("test", 100L);
            store.addFile(file);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(file), List.of());

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(readyForGCFileReport("test", updateTime));
        }

        @Test
        void shouldReportTwoActiveFiles() throws Exception {
            // Given
            FileInfo file1 = factory.rootFile("file1", 100L);
            FileInfo file2 = factory.rootFile("file2", 100L);
            store.addFiles(List.of(file1, file2));

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(wholeFilesReport(file1, file2));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileInfo rootFile = factory.rootFile("file", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(splitFileReport("file", updateTime, leftFile, rightFile));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitionsWithOneReadyForGC() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileInfo rootFile = factory.rootFile("file", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(leftFile), List.of());

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(wholeFilesReport(rightFile));
        }

        private void splitPartition(String parentId, String leftId, String rightId, long splitPoint) {
            partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint);
            factory = FileInfoFactory.fromUpdatedAt(partitions.buildTree(), updateTime);
        }

        private FileInfo splitFile(FileInfo parentFile, String childPartitionId) {
            return SplitFileInfo.referenceForChildPartition(parentFile, childPartitionId)
                    .toBuilder().lastStateStoreUpdateTime(updateTime).build();
        }
    }

    @Nested
    @DisplayName("Atomically update files")
    class AtomicallyUpdateFiles {
        @Test
        public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFile() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            List<String> filesToMoveToReadyForGC = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileInfo fileInfo = FileInfo.wholeFile()
                        .filename("file" + i)
                        .partitionId("7")
                        .numberOfRecords(100L)
                        .build();
                filesToMoveToReadyForGC.add(fileInfo.getFilename());
                dynamoDBStateStore.addFile(fileInfo);
            }
            FileInfo newFileInfo = FileInfo.wholeFile()
                    .filename("file-new")
                    .partitionId("7")
                    .numberOfRecords(100L)
                    .build();

            // When
            dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile("7", filesToMoveToReadyForGC, newFileInfo);

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(newFileInfo);
            assertThat(dynamoDBStateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                    .containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
        }

        @Test
        public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFilesForSplittingJob() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            List<String> filesToMoveToReadyForGC = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileInfo fileInfo = FileInfo.wholeFile()
                        .filename("file" + i)
                        .partitionId("7")
                        .numberOfRecords(100L)
                        .build();
                filesToMoveToReadyForGC.add(fileInfo.getFilename());
                dynamoDBStateStore.addFile(fileInfo);
            }
            FileInfo newLeftFileInfo = FileInfo.wholeFile()
                    .filename("file-left-new")
                    .partitionId("7")
                    .numberOfRecords(100L)
                    .build();
            FileInfo newRightFileInfo = FileInfo.wholeFile()
                    .filename("file-right-new")
                    .partitionId("7")
                    .numberOfRecords(100L)
                    .build();

            // When
            dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("7", filesToMoveToReadyForGC, newLeftFileInfo, newRightFileInfo);

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactlyInAnyOrder(newLeftFileInfo, newRightFileInfo);
            assertThat(dynamoDBStateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                    .containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
        }

        @Test
        public void atomicallyUpdateStatusToReadyForGCAndCreateNewActiveFileShouldFailIfFilesNotActive() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            List<FileInfo> files = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileInfo fileInfo = FileInfo.wholeFile()
                        .filename("file" + i)
                        .partitionId("7")
                        .numberOfRecords(100L)
                        .build();
                files.add(fileInfo);
            }
            //  - One of the files (file4) is not active
            FileInfo updatedFileInfo = files.remove(3);
            dynamoDBStateStore.addFile(updatedFileInfo);
            dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(updatedFileInfo), files);
            FileInfo newFileInfo = FileInfo.wholeFile()
                    .filename("file-new")
                    .partitionId("7")
                    .numberOfRecords(100L)
                    .build();

            // When / Then
            assertThatThrownBy(() ->
                    dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile("7", List.of("file4"), newFileInfo))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldAtomicallyUpdateJobStatusOfFiles() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            List<FileInfo> files = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileInfo fileInfo = FileInfo.wholeFile()
                        .filename("file" + i)
                        .partitionId("8")
                        .numberOfRecords(100L)
                        .build();
                files.add(fileInfo);
                dynamoDBStateStore.addFile(fileInfo);
            }
            String jobId = UUID.randomUUID().toString();

            // When
            dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);

            // Then
            assertThat(dynamoDBStateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId", "lastStateStoreUpdateTime")
                    .containsExactlyInAnyOrderElementsOf(files)
                    .extracting(FileInfo::getJobId).containsOnly(jobId);
            assertThat(dynamoDBStateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
        }

        @Test
        public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            List<FileInfo> files = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileInfo fileInfo = FileInfo.wholeFile()
                        .filename("file" + i)
                        .partitionId("9")
                        .numberOfRecords(100L)
                        .jobId("compactionJob")
                        .build();
                files.add(fileInfo);
                dynamoDBStateStore.addFile(fileInfo);
            }
            String jobId = UUID.randomUUID().toString();

            // When / Then
            assertThatThrownBy(() ->
                    dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldNotAtomicallyUpdateJobStatusOfFilesIfFileInfoNotPresent() {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            List<FileInfo> files = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileInfo fileInfo = FileInfo.wholeFile()
                        .filename("file" + i)
                        .partitionId("8")
                        .numberOfRecords(1000L)
                        .build();
                files.add(fileInfo);
            }
            String jobId = UUID.randomUUID().toString();

            // When / Then
            assertThatThrownBy(() -> dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                    .isInstanceOf(StateStoreException.class);
        }
    }

    @Nested
    @DisplayName("Initialise partitions")
    class InitialisePartitions {

        @Test
        public void shouldCorrectlyInitialisePartitionsWithLongKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList(100L))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyInitialisePartitionsWithStringKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new StringType());
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList("B"))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyInitialisePartitionsWithByteArrayKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
            byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
            byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyInitialisePartitionsWithMultidimensionalKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
            byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
            byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyStoreNonLeafPartitionWithByteArrayKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
            byte[] min = new byte[]{1, 2, 3, 4};
            byte[] max = new byte[]{5, 6, 7, 8, 9};
            Range range = new RangeFactory(schema).createRange("key", min, max);
            Partition partition = Partition.builder()
                    .rowKeyTypes(schema.getRowKeyTypes())
                    .region(new Region(range))
                    .id("id")
                    .leafPartition(false)
                    .parentPartitionId("P")
                    .childPartitionIds(new ArrayList<>())
                    .dimension(0)
                    .build();
            StateStore dynamoDBStateStore = getStateStore(schema, Collections.singletonList(partition));

            // When
            Partition retrievedPartition = dynamoDBStateStore.getAllPartitions().get(0);

            // Then
            assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key").getMin());
            assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key").getMax());
            assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
            assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
            assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
            assertThat(retrievedPartition.getDimension()).isEqualTo(partition.getDimension());
        }

        // TODO shouldCorrectlyStorePartitionWithMultidimensionalKeyType

        @Test
        public void shouldReturnCorrectPartitionToFileMapping() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore dynamoDBStateStore = getStateStore(schema);
            List<FileInfo> files = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                FileInfo fileInfo = FileInfo.wholeFile()
                        .filename("file" + i)
                        .partitionId("" + (i % 5))
                        .numberOfRecords(100L)
                        .build();
                files.add(fileInfo);
                dynamoDBStateStore.addFile(fileInfo);
            }

            // When
            Map<String, List<String>> partitionToFileMapping = dynamoDBStateStore.getPartitionToActiveFilesMap();

            // Then
            assertThat(partitionToFileMapping.entrySet()).hasSize(5);
            for (int i = 0; i < 5; i++) {
                assertThat(partitionToFileMapping.get("" + i)).hasSize(2);
                Set<String> expected = new HashSet<>();
                expected.add(files.get(i).getFilename());
                expected.add(files.get(i + 5).getFilename());
                assertThat(new HashSet<>(partitionToFileMapping.get("" + i))).isEqualTo(expected);
            }
        }

        @Test
        public void shouldReturnAllPartitions() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();

            PartitionTree tree = new PartitionsBuilder(schema).rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 100L)
                    .splitToNewChildren("left", "id1", "id2", 1L)
                    .splitToNewChildren("right", "id3", "id4", 200L).buildTree();

            StateStore dynamoDBStateStore = getStateStore(schema, tree.getAllPartitions());

            // When / Then
            assertThat(dynamoDBStateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldReturnLeafPartitions() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();

            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .buildTree();

            PartitionTree intermediateTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id1", "id2", 1L)
                    .buildTree();

            PartitionTree expectedTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id1", "id2", 1L)
                    .splitToNewChildren("id2", "id3", "id4", 9L).buildTree();
            DynamoDBStateStore stateStore = getStateStore(schema, tree.getAllPartitions());

            // When
            stateStore.atomicallyUpdatePartitionAndCreateNewOnes(intermediateTree.getRootPartition(), intermediateTree.getPartition("id1"), intermediateTree.getPartition("id2"));
            stateStore.atomicallyUpdatePartitionAndCreateNewOnes(expectedTree.getPartition("id2"), expectedTree.getPartition("id3"), expectedTree.getPartition("id4"));

            // Then
            assertThat(stateStore.getLeafPartitions())
                    .containsExactlyInAnyOrderElementsOf(expectedTree.getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList()));
        }

        @Test
        public void shouldUpdatePartitions() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore dynamoDBStateStore = getStateStore(schema);

            // When
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "child1", "child2", 0L)
                    .buildTree();

            dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getRootPartition(), tree.getPartition("child1"), tree.getPartition("child2"));

            // Then
            assertThat(dynamoDBStateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore dynamoDBStateStore = getStateStore(schema);

            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "child1", "child2", 0L)
                    .buildTree();

            dynamoDBStateStore.initialise(tree.getAllPartitions());

            // When / Then
            //  - Attempting to split something that has already been split should fail
            assertThatThrownBy(() ->
                    dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("root"), tree.getPartition("child1"), tree.getPartition("child2")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldThrowExceptionWithPartitionSplitRequestWhereParentIsLeaf() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            StateStore dynamoDBStateStore = getStateStore(schema);
            Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
            parentPartition = parentPartition.toBuilder().childPartitionIds(Arrays.asList("child1", "child2")).build();
            Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition1 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child1")
                    .region(region1)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();
            Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition2 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child2")
                    .region(region2)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();

            // When / Then
            Partition finalParentPartition = parentPartition;
            assertThatThrownBy(() ->
                    dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            StateStore dynamoDBStateStore = getStateStore(schema);
            Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
            parentPartition = parentPartition.toBuilder()
                    .leafPartition(false)
                    .childPartitionIds(Arrays.asList("child3", "child2")) // Wrong children
                    .build();
            Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition1 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child1")
                    .region(region1)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();
            Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition2 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child2")
                    .region(region2)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();

            // When / Then
            Partition finalParentPartition = parentPartition;
            assertThatThrownBy(() ->
                    dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            StateStore dynamoDBStateStore = getStateStore(schema);
            Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
            parentPartition = parentPartition.toBuilder()
                    .leafPartition(false)
                    .childPartitionIds(Arrays.asList("child1", "child2"))
                    .build();
            Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition1 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child1")
                    .region(region1)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("notparent") // Wrong parent
                    .build();
            Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition2 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child2")
                    .region(region2)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();

            // When / Then
            Partition finalParentPartition = parentPartition;
            assertThatThrownBy(() ->
                    dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore dynamoDBStateStore = getStateStore(schema);

            // When
            List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

            // Then
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore dynamoDBStateStore = getStateStore(schema);

            // When
            List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

            // Then
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore dynamoDBStateStore = getStateStore(schema);

            // When
            List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

            // Then
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore dynamoDBStateStore = getStateStore(schema);

            // When
            List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());

            // Then
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        void shouldNotReinitialisePartitionsWhenAFileIsPresent() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L)
                    .buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L)
                    .buildTree();
            StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());
            stateStore.addFile(FileInfoFactory.from(treeBefore).partitionFile("before2", 100L));

            // When / Then
            assertThatThrownBy(() -> stateStore.initialise(treeAfter.getAllPartitions()))
                    .isInstanceOf(StateStoreException.class);
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(treeBefore.getAllPartitions());
        }

        @Test
        public void shouldReinitialisePartitionsWhenNoFilesArePresent() throws StateStoreException {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L)
                    .buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L)
                    .buildTree();
            StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());

            // When
            stateStore.initialise(treeAfter.getAllPartitions());

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(treeAfter.getAllPartitions());
        }
    }
}
