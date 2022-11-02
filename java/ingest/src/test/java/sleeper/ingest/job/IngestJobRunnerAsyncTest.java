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

package sleeper.ingest.job;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.IngestProperties;
import sleeper.ingest.IngestRecordsTestBase;
import sleeper.ingest.IngestResult;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.createPartitionListWithLongType;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSingleRecord;

public class IngestJobRunnerAsyncTest extends IngestRecordsTestBase {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3);
    private StateStore stateStore;
    private static final String BUCKET_NAME = "test-bucket-name";

    @Before
    public void setup() throws StateStoreException {
        stateStore = createStateStore();
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(BUCKET_NAME);
    }

    @After
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    @Test
    public void shouldIngestMultipleRecordsWithArrayListBackedAsyncCoordinator() throws StateStoreException, IteratorException, IOException {
        // Given we have an ArrayList backed Async coordinator
        IngestCoordinator<Record> ingestCoordinator = createArrayBackedAsyncCoordinator();

        // And we have a list of records to ingest 
        List<Record> recordList = getRecords();

        // When we create and run an IngestJobRunner
        IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, recordList.iterator());
        IngestResult result = jobRunner.run();
        // Then the files should be ingested
        assertThat(result.getNumberOfRecords()).isEqualTo(2L);
        assertThat(result.getFileInfoList()).containsAll(stateStore.getActiveFiles());
    }

    @Test
    public void shouldIngestSingleRecordWithArrayListBackedAsyncCoordinator() throws StateStoreException, IteratorException, IOException {
        // Given we have an ArrayList backed Async coordinator
        IngestCoordinator<Record> ingestCoordinator = createArrayBackedAsyncCoordinator();

        // And we have a list of records to ingest 
        List<Record> recordList = getSingleRecord();

        // When we create and run an IngestJobRunner
        IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, recordList.iterator());
        IngestResult result = jobRunner.run();
        // Then the files should be ingested
        assertThat(result.getNumberOfRecords()).isEqualTo(1L);
        assertThat(result.getFileInfoList()).containsAll(stateStore.getActiveFiles());
    }

    @Test
    public void shouldNotIngestRecordsWithEmptyIteratorAndArrayListBackedAsyncCoordinator() throws StateStoreException, IteratorException, IOException {
        // Given we have an ArrayList backed Async coordinator
        IngestCoordinator<Record> ingestCoordinator = createArrayBackedAsyncCoordinator();

        // When we create and run an IngestJobRunner with an empty iterator
        IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, Collections.emptyIterator());
        IngestResult result = jobRunner.run();

        // Then no files should be ingested
        assertThat(result.getNumberOfRecords()).isEqualTo(0L);
        assertThat(result.getFileInfoList()).isEmpty();
    }

    @Test
    public void shouldIngestMultipleRecordsWithArrowBackedAsyncCoordinator() throws StateStoreException, IteratorException, IOException {
        IngestResult result;
        try (BufferAllocator bufferAllocator = new RootAllocator()) {
            // Given we have an Arrow backed Async coordinator
            IngestCoordinator<Record> ingestCoordinator = createArrowBackedAsyncCoordinator(bufferAllocator);

            // And we have a list of records to ingest 
            List<Record> recordList = getRecords();

            // When we create and run an IngestJobRunner
            IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, recordList.iterator());
            result = jobRunner.run();
        }
        // Then the files should be ingested
        assertThat(result.getNumberOfRecords()).isEqualTo(2L);
        assertThat(result.getFileInfoList()).containsAll(stateStore.getActiveFiles());
    }

    @Test
    public void shouldIngestSingleRecordWithArrowBackedAsyncCoordinator() throws StateStoreException, IteratorException, IOException {
        IngestResult result;
        try (BufferAllocator bufferAllocator = new RootAllocator()) {
            // Given we have an Arrow backed Async coordinator
            IngestCoordinator<Record> ingestCoordinator = createArrowBackedAsyncCoordinator(bufferAllocator);

            // And we have a list of records to ingest 
            List<Record> recordList = getSingleRecord();

            // When we create and run an IngestJobRunner
            IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, recordList.iterator());
            result = jobRunner.run();
        }
        // Then the files should be ingested
        assertThat(result.getNumberOfRecords()).isEqualTo(1L);
        assertThat(result.getFileInfoList()).containsAll(stateStore.getActiveFiles());
    }

    @Test
    public void shouldNotIngestRecordsWithEmptyIteratorAndArrowBackedAsyncCoordinator() throws StateStoreException, IteratorException, IOException {
        IngestResult result;
        try (BufferAllocator bufferAllocator = new RootAllocator()) {
            // Given we have an Arrow backed Async coordinator
            IngestCoordinator<Record> ingestCoordinator = createArrowBackedAsyncCoordinator(bufferAllocator);

            // When we create and run an IngestJobRunner with an empty iterator
            IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, Collections.emptyIterator());
            result = jobRunner.run();
        }
        // Then no files should be ingested
        assertThat(result.getNumberOfRecords()).isEqualTo(0L);
        assertThat(result.getFileInfoList()).isEmpty();
    }

    private IngestCoordinator<Record> createArrayBackedAsyncCoordinator() {
        IngestProperties ingestProperties = defaultPropertiesBuilder(stateStore, schema)
                .hadoopConfiguration(AWS_EXTERNAL_RESOURCE.getHadoopConfiguration())
                .build();
        return StandardIngestCoordinator.asyncS3WriteBackedByArrayList(
                ingestProperties,
                BUCKET_NAME,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient());
    }

    private IngestCoordinator<Record> createArrowBackedAsyncCoordinator(BufferAllocator bufferAllocator) {
        IngestProperties ingestProperties = defaultPropertiesBuilder(stateStore, schema)
                .hadoopConfiguration(AWS_EXTERNAL_RESOURCE.getHadoopConfiguration())
                .maxRecordsToWriteLocally(16 * 1024 * 1024L)
                .build();
        return StandardIngestCoordinator.asyncS3WriteBackedByArrow(
                ingestProperties,
                BUCKET_NAME,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                bufferAllocator,
                10,
                16 * 1024 * 1024L,
                16 * 1024 * 1024L,
                16 * 1024 * 1024L);
    }

    private StateStore createStateStore() throws StateStoreException {
        return getStateStore(schema,
                createPartitionListWithLongType(schema, field));
    }
}
