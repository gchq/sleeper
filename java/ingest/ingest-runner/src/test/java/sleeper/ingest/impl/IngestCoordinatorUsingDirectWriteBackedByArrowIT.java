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

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.IngestTestHelper;
import sleeper.ingest.testutils.RecordGenerator;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IngestCoordinatorUsingDirectWriteBackedByArrowIT {
    @RegisterExtension
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.DYNAMODB);
    private static final String DATA_BUCKET_NAME = "databucket";
    @TempDir
    public Path temporaryFolder;

    @BeforeEach
    public void before() {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(DATA_BUCKET_NAME);
    }

    @AfterEach
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
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

        test(recordListAndSchema, tree, arrow -> arrow
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(128 * 1024 * 1024L)
        ).ingestAndVerify(keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
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

        test(recordListAndSchema, tree, arrow -> arrow
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L)
        ).ingestAndVerify(keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() throws Exception {
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

        IngestTestHelper<Record> test = test(recordListAndSchema, tree, arrow -> arrow
                .workingBufferAllocatorBytes(32 * 1024L)
                .batchBufferAllocatorBytes(1024 * 1024L)
                .maxNoOfBytesToWriteLocally(64 * 1024 * 1024L));
        assertThatThrownBy(() ->
                test.ingestAndVerify(keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private IngestTestHelper<Record> test(RecordGenerator.RecordListAndSchema recordListAndSchema,
                                          PartitionTree tree,
                                          Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig) throws Exception {
        return IngestTestHelper.from(temporaryFolder,
                        AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                        recordListAndSchema)
                .createStateStore(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(), tree)
                .directWrite()
                .backedByArrow(arrowConfig);
    }
}
