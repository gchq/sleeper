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
package sleeper.ingest.impl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;
import static sleeper.ingest.testutils.ResultVerifier.assertOnSketch;
import static sleeper.ingest.testutils.ResultVerifier.createFieldToItemSketchMap;
import static sleeper.ingest.testutils.ResultVerifier.readFieldToItemSketchMap;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

@Testcontainers
class IngestCoordinatorUsingDirectWriteBackedByArrowIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB,
            AmazonDynamoDBClientBuilder.standard());
    private static final String TABLE_NAME = "test-table";
    @TempDir
    public Path temporaryFolder;

    @AfterEach
    public void after() {
        dynamoDB.deleteTable(TABLE_NAME + "-af");
        dynamoDB.deleteTable(TABLE_NAME + "-rgcf");
        dynamoDB.deleteTable(TABLE_NAME + "-p");
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();

        ingestAndVerify(recordListAndSchema, tree, arrow -> arrow
                        .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                        .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                        .maxNoOfBytesToWriteLocally(128 * 1024 * 1024L),
                keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();

        ingestAndVerify(recordListAndSchema, tree, arrow -> arrow
                        .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                        .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                        .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L),
                keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();

        assertThatThrownBy(() ->
                ingestAndVerify(recordListAndSchema, tree, arrow -> arrow
                                .workingBufferAllocatorBytes(32 * 1024L)
                                .batchBufferAllocatorBytes(1024 * 1024L)
                                .maxNoOfBytesToWriteLocally(64 * 1024 * 1024L),
                        keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private void ingestAndVerify(RecordGenerator.RecordListAndSchema recordListAndSchema,
                                 PartitionTree tree,
                                 Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig,
                                 Function<Key, Integer> keyToPartitionNoMappingFn,
                                 Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap) throws Exception {
        Schema schema = recordListAndSchema.sleeperSchema;
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(TABLE_NAME, schema, dynamoDB).create();
        stateStore.initialise(tree.getAllPartitions());
        Configuration configuration = AwsExternalResource.getHadoopConfiguration(localStackContainer);
        ParquetConfiguration parquetConfiguration = parquetConfiguration(schema, configuration);

        String localWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        String ingestToDirectory = createTempDirectory(temporaryFolder, null).toString();
        PartitionFileWriterFactory partitionFileWriterFactory = DirectPartitionFileWriterFactory.from(
                parquetConfiguration, ingestToDirectory);
        ArrowRecordBatchFactory<Record> recordBatchFactory = createArrowRecordBatchFactory(
                schema, localWorkingDirectory, arrowConfig);

        try (IngestCoordinator<Record> ingestCoordinator = standardIngestCoordinator(
                stateStore, schema, recordBatchFactory, partitionFileWriterFactory)) {
            for (Record write : recordListAndSchema.recordList) {
                ingestCoordinator.write(write);
            }
        }

        verify(localWorkingDirectory, schema, stateStore, recordListAndSchema.recordList,
                configuration, keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
    }

    private static void verify(String localWorkingDirectory, Schema schema, StateStore stateStore, List<Record> expectedRecords,
                               Configuration configuration, Function<Key, Integer> keyToPartitionNoMappingFn,
                               Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap) throws StateStoreException {
        assertThat(Path.of(localWorkingDirectory)).isEmptyDirectory();

        PartitionTree partitionTree = new PartitionTree(schema, stateStore.getAllPartitions());

        Map<Integer, List<Record>> partitionNoToExpectedRecordsMap = expectedRecords.stream()
                .collect(Collectors.groupingBy(
                        record -> keyToPartitionNoMappingFn.apply(Key.create(record.getValues(schema.getRowKeyFieldNames())))));

        Map<String, List<FileInfo>> partitionIdToFileInfosMap = stateStore.getActiveFiles().stream()
                .collect(Collectors.groupingBy(FileInfo::getPartitionId));

        Map<String, Integer> partitionIdToPartitionNoMap = partitionNoToExpectedRecordsMap.entrySet().stream()
                .map(entry -> {
                    Key keyOfFirstRecord = Key.create(entry.getValue().get(0).getValues(schema.getRowKeyFieldNames()));
                    return new AbstractMap.SimpleEntry<>(partitionTree.getLeafPartition(keyOfFirstRecord).getId(), entry.getKey());
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Integer, List<FileInfo>> partitionNoToFileInfosMap = partitionIdToFileInfosMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> partitionIdToPartitionNoMap.get(entry.getKey()),
                        Map.Entry::getValue));

        int expectedTotalNoOfFiles = partitionNoToExpectedNoOfFilesMap.values().stream()
                .mapToInt(Integer::valueOf)
                .sum();

        Set<Integer> allPartitionNoSet = Stream.of(
                        partitionNoToFileInfosMap.keySet().stream(),
                        partitionNoToExpectedNoOfFilesMap.keySet().stream(),
                        partitionNoToExpectedRecordsMap.keySet().stream())
                .flatMap(Function.identity())
                .collect(Collectors.toSet());

        assertThat(stateStore.getActiveFiles()).hasSize(expectedTotalNoOfFiles);
        assertThat(allPartitionNoSet).allMatch(partitionNoToExpectedNoOfFilesMap::containsKey);

        allPartitionNoSet.forEach(partitionNo -> verifyPartition(schema, configuration,
                partitionNoToFileInfosMap.getOrDefault(partitionNo, Collections.emptyList()),
                partitionNoToExpectedNoOfFilesMap.get(partitionNo),
                partitionNoToExpectedRecordsMap.getOrDefault(partitionNo, Collections.emptyList())));
    }

    private static void verifyPartition(Schema schema, Configuration hadoopConfiguration, List<FileInfo> partitionFileInfoList,
                                        int expectedNoOfFiles, List<Record> expectedRecords) {
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(schema, partitionFileInfoList, hadoopConfiguration);

        assertThat(partitionFileInfoList).hasSize(expectedNoOfFiles);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);

        // In some situations, check that the file min and max match the min and max of dimension 0
        if (expectedNoOfFiles == 1 &&
                schema.getRowKeyFields().get(0).getType() instanceof LongType) {
            String rowKeyFieldNameDimension0 = schema.getRowKeyFieldNames().get(0);
            Key minRowKeyDimension0 = expectedRecords.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .min(Comparator.naturalOrder())
                    .map(Key::create)
                    .orElseThrow();
            Key maxRowKeyDimension0 = expectedRecords.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .max(Comparator.naturalOrder())
                    .map(Key::create)
                    .orElseThrow();
            partitionFileInfoList.forEach(fileInfo -> {
                assertThat(fileInfo.getMinRowKey()).isEqualTo(minRowKeyDimension0);
                assertThat(fileInfo.getMaxRowKey()).isEqualTo(maxRowKeyDimension0);
            });
        }

        if (expectedNoOfFiles > 0) {
            Map<Field, ItemsSketch> expectedFieldToItemsSketchMap = createFieldToItemSketchMap(schema, expectedRecords);
            Map<Field, ItemsSketch> savedFieldToItemsSketchMap = readFieldToItemSketchMap(schema, partitionFileInfoList, hadoopConfiguration);
            schema.getRowKeyFields().forEach(field ->
                    assertOnSketch(field, expectedFieldToItemsSketchMap.get(field), savedFieldToItemsSketchMap.get(field)));
        }
    }

    private static ArrowRecordBatchFactory<Record> createArrowRecordBatchFactory(
            Schema schema, String localWorkingDirectory, Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig) {
        ArrowRecordBatchFactory.Builder<Record> builder = ArrowRecordBatchFactory.builder().schema(schema)
                .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                .localWorkingDirectory(localWorkingDirectory)
                .recordWriter(new ArrowRecordWriterAcceptingRecords());
        arrowConfig.accept(builder);
        return builder.build();
    }
}
