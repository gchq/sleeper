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
package sleeper.ingest.job.commit;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.InMemoryTableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.finishedIngestJob;

public class IngestJobCommitterTest {
    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2024-06-07T15:08:00Z");
    private final Schema schema = schemaWithKey("key");
    private final FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(
            new PartitionsBuilder(schema).rootFirst("root").buildTree(), DEFAULT_UPDATE_TIME);
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore();
    private final Map<String, StateStore> stateStoreByTableId = new HashMap<>();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final InMemoryIngestJobStatusStore statusStore = new InMemoryIngestJobStatusStore();

    @Test
    void shouldCommitIngestJobsOnDifferentTables() throws Exception {
        // Given
        TableProperties table1 = createTable();
        TableProperties table2 = createTable();
        IngestJob job1 = createJobWithTableAndFiles("test-job-1", table1.getStatus(), "file1.parquet");
        IngestJob job2 = createJobWithTableAndFiles("test-job-2", table2.getStatus(), "file2.parquet");
        FileReference jobFile1 = factory.rootFile("file1.parquet", 100L);
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(100, 100),
                Instant.parse("2024-05-01T10:58:00Z"), Duration.ofMinutes(1));
        FileReference jobFile2 = factory.rootFile("file2.parquet", 400L);
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(400, 400),
                Instant.parse("2024-05-01T10:58:30Z"), Duration.ofMinutes(1));
        IngestJobCommitRequest commitRequest1 = runIngestJobOnTask("test-task", job1, List.of(jobFile1), summary1);
        IngestJobCommitRequest commitRequest2 = runIngestJobOnTask("test-task", job2, List.of(jobFile2), summary2);

        // When
        IngestJobCommitter jobCommitter = jobCommitter();
        jobCommitter.apply(commitRequest1);
        jobCommitter.apply(commitRequest2);

        // Then
        StateStore stateStore1 = stateStore(table1);
        StateStore stateStore2 = stateStore(table2);
        IngestJobStatus jobStatus1 = statusStore.getJob(job1.getId()).orElseThrow();
        IngestJobStatus jobStatus2 = statusStore.getJob(job2.getId()).orElseThrow();
        assertThat(jobStatus1).isEqualTo(finishedIngestJob(job1, "test-task", summary1));
        assertThat(jobStatus2).isEqualTo(finishedIngestJob(job2, "test-task", summary2));
        assertThat(stateStore1.getFileReferences()).containsExactly(jobFile1);
        assertThat(stateStore2.getFileReferences()).containsExactly(jobFile2);
    }

    private IngestJobCommitter jobCommitter() {
        return new IngestJobCommitter(statusStore, stateStoreByTableId::get);
    }

    private IngestJobCommitRequest runIngestJobOnTask(String taskId, IngestJob job, List<FileReference> fileReferences, RecordsProcessedSummary summary) {
        statusStore.jobStarted(IngestJobStartedEvent.ingestJobStarted(taskId, job, summary.getStartTime()));
        return new IngestJobCommitRequest(job, taskId, fileReferences, summary);
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tablePropertiesStore.createTable(tableProperties);
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(tableProperties.getSchema());
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStoreByTableId.put(tableProperties.get(TABLE_ID), stateStore);
        return tableProperties;
    }

    private StateStore stateStore(TableProperties table) {
        return stateStoreByTableId.get(table.get(TABLE_ID));
    }
}
