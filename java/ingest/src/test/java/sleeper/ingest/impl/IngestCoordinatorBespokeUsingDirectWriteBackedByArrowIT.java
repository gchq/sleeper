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
package sleeper.ingest.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorException;
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.IngestProperties;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.PartitionedTableCreator;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IngestCoordinatorBespokeUsingDirectWriteBackedByArrowIT {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.DYNAMODB);
    private static final String DATA_BUCKET_NAME = "databucket";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void before() {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(DATA_BUCKET_NAME);
    }

    @After
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws StateStoreException, IOException, IteratorException, ObjectFactoryException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrow(
                recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                16 * 1024 * 1024L,
                4 * 1024 * 1024L,
                128 * 1024 * 1024L);
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws StateStoreException, IOException, IteratorException, ObjectFactoryException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrow(
                recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                16 * 1024 * 1024L,
                4 * 1024 * 1024L,
                16 * 1024 * 1024L);
    }

    @Test
    public void shouldError() {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThatThrownBy(() ->
                ingestAndVerifyUsingDirectWriteBackedByArrow(
                        recordListAndSchema,
                        keyAndDimensionToSplitOnInOrder,
                        keyToPartitionNoMappingFn,
                        partitionNoToExpectedNoOfFilesMap,
                        32 * 1024L,
                        1024 * 1024L,
                        64 * 1024 * 1024L))
                .isInstanceOf(NullPointerException.class);
    }

    private void ingestAndVerifyUsingDirectWriteBackedByArrow(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes) throws IOException, ObjectFactoryException, StateStoreException, IteratorException {
        BufferAllocator bufferAllocator = new RootAllocator();
        StateStore stateStore = PartitionedTableCreator.createStateStore(
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                recordListAndSchema.sleeperSchema,
                keyAndDimensionToSplitOnInOrder);
        String objectFactoryLocalWorkingDirectory = temporaryFolder.newFolder().getAbsolutePath();
        String ingestLocalWorkingDirectory = temporaryFolder.newFolder().getAbsolutePath();
        IngestProperties properties = defaultPropertiesBuilder(objectFactoryLocalWorkingDirectory, stateStore, recordListAndSchema.sleeperSchema, null, ingestLocalWorkingDirectory)
                .filePathPrefix("s3a://" + DATA_BUCKET_NAME)
                .maxRecordsToWriteLocally(localStoreBytes).build();
        IngestCoordinator<sleeper.core.record.Record> ingestCoordinator = StandardIngestCoordinator.directWriteBackedByArrow(
                properties,
                bufferAllocator,
                128,
                arrowWorkingBytes,
                arrowBatchBytes,
                arrowBatchBytes
        );

        try {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        } finally {
            ingestCoordinator.close();
        }

        ResultVerifier.verify(
                stateStore,
                recordListAndSchema.sleeperSchema,
                keyToPartitionNoMappingFn,
                recordListAndSchema.recordList,
                partitionNoToExpectedNoOfFilesMap,
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                ingestLocalWorkingDirectory);
    }

    private static IngestProperties.Builder defaultPropertiesBuilder(String objectFactoryLocalWorkingDirectory,
                                                                     StateStore stateStore,
                                                                     Schema sleeperSchema,
                                                                     String sleeperIteratorClassName,
                                                                     String ingestLocalWorkingDirectory) throws IOException, ObjectFactoryException {
        return IngestProperties.builder()
                .objectFactory(new ObjectFactory(new InstanceProperties(), null, objectFactoryLocalWorkingDirectory))
                .localDir(ingestLocalWorkingDirectory)
                .maxRecordsToWriteLocally(10L)
                .maxInMemoryBatchSize(1000)
                .rowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .pageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .compressionCodec("zstd")
                .stateStore(stateStore)
                .schema(sleeperSchema)
                .hadoopConfiguration(AWS_EXTERNAL_RESOURCE.getHadoopConfiguration())
                .iteratorClassName(sleeperIteratorClassName)
                .ingestPartitionRefreshFrequencyInSecond(Integer.MAX_VALUE);
    }
}
