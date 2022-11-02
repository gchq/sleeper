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
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.IngestProperties;
import sleeper.ingest.IngestRecordsTestBase;
import sleeper.ingest.IngestResult;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.createPartitionListWithLongType;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSingleRecord;

public class IngestJobRunnerDirectTest extends IngestRecordsTestBase {
    private StateStore stateStore;

    @Before
    public void setup() throws StateStoreException {
        stateStore = createStateStore();
    }

    @Test
    public void shouldIngestMultipleRecordsWithArrayListBackedDirectCoordinator() throws StateStoreException, IteratorException, IOException {
        // Given we have an ArrayList backed Direct coordinator
        IngestCoordinator<Record> ingestCoordinator = createArrayListBackedDirectCoordinator();
        // And we have a list of files to ingest 
        List<Record> recordList = getRecords();

        // When we create and run an IngestJobRunner
        IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, recordList.iterator());
        IngestResult result = jobRunner.run();

        // Then the files should be ingested
        assertThat(result.getNumberOfRecords())
                .isEqualTo(2L);
        assertThat(result.getFileInfoList())
                .containsAll(stateStore.getActiveFiles());
    }

    @Test
    public void shouldIngestSingleRecordWithArrayListBackedDirectCoordinator() throws StateStoreException, IteratorException, IOException {
        // Given we have an ArrayList backed Direct coordinator
        IngestCoordinator<Record> ingestCoordinator = createArrayListBackedDirectCoordinator();
        // And we have a list of files to ingest 
        List<Record> recordList = getSingleRecord();

        // When we create and run an IngestJobRunner
        IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, recordList.iterator());
        IngestResult result = jobRunner.run();

        // Then the files should be ingested
        assertThat(result.getNumberOfRecords())
                .isEqualTo(1L);
        assertThat(result.getFileInfoList())
                .containsAll(stateStore.getActiveFiles());
    }

    @Test
    public void shouldNotIngestRecordsWithEmptyIteratorAndArrayListBackedDirectCoordinator() throws StateStoreException, IteratorException, IOException {
        // Given we have an ArrayList backed Direct coordinator
        IngestCoordinator<Record> ingestCoordinator = createArrayListBackedDirectCoordinator();

        // When we create and run an IngestJobRunner with an empty iterator
        IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, Collections.emptyIterator());
        IngestResult result = jobRunner.run();

        // Then the files should be ingested
        assertThat(result.getNumberOfRecords())
                .isEqualTo(0L);
        assertThat(result.getFileInfoList())
                .isEmpty();
    }

    @Test
    public void shouldIngestMultipleRecordsWithArrowBackedDirectCoordinator() throws StateStoreException, IteratorException, IOException {
        IngestResult result;
        try (BufferAllocator bufferAllocator = new RootAllocator()) {
            // Given we have an Arrow backed Direct coordinator
            IngestCoordinator<Record> ingestCoordinator = createArrowBackedDirectCoordinator(bufferAllocator);

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
    public void shouldIngestSingleRecordWithArrowBackedDirectCoordinator() throws StateStoreException, IteratorException, IOException {
        IngestResult result;
        try (BufferAllocator bufferAllocator = new RootAllocator()) {
            // Given we have an Arrow backed Direct coordinator
            IngestCoordinator<Record> ingestCoordinator = createArrowBackedDirectCoordinator(bufferAllocator);

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
    public void shouldNotIngestRecordsWithEmptyIteratorAndArrowBackedDirectCoordinator() throws StateStoreException, IteratorException, IOException {
        IngestResult result;
        try (BufferAllocator bufferAllocator = new RootAllocator()) {
            // Given we have an Arrow backed Direct coordinator
            IngestCoordinator<Record> ingestCoordinator = createArrowBackedDirectCoordinator(bufferAllocator);

            // When we create and run an IngestJobRunner with an empty iterator
            IngestJobRunner jobRunner = IngestJobRunner.with(ingestCoordinator, Collections.emptyIterator());
            result = jobRunner.run();
        }
        // Then no files should be ingested
        assertThat(result.getNumberOfRecords()).isEqualTo(0L);
        assertThat(result.getFileInfoList()).isEmpty();
    }

    private IngestCoordinator<Record> createArrayListBackedDirectCoordinator() throws StateStoreException {
        return StandardIngestCoordinator.directWriteBackedByArrayList(defaultIngestProperties());
    }

    private IngestCoordinator<Record> createArrowBackedDirectCoordinator(BufferAllocator bufferAllocator) throws StateStoreException {
        return StandardIngestCoordinator.directWriteBackedByArrow(
                defaultIngestProperties(),
                bufferAllocator,
                10,
                16 * 1024 * 1024L,
                16 * 1024 * 1024L,
                16 * 1024 * 1024L);
    }

    private IngestProperties defaultIngestProperties() throws StateStoreException {
        return defaultPropertiesBuilder(getStateStore(schema), schema)
                .hadoopConfiguration(defaultHadoopConfiguration())
                .build();
    }

    private static Configuration defaultHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");
        return conf;
    }

    private StateStore createStateStore() throws StateStoreException {
        return getStateStore(schema,
                createPartitionListWithLongType(schema, field));
    }
}
