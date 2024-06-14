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
package sleeper.commit;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobStatusFrom;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionStatus;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJobOnTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

public class StateStoreCommitterTest {
    private Schema schema = schemaWithKey("key");
    private final InMemoryCompactionJobStatusStore compactionJobStatusStore = new InMemoryCompactionJobStatusStore();
    private final Map<String, StateStore> stateStoreByTableId = new HashMap<>();

    @Test
    void shouldApplyCompactionCommitRequest() throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
        Instant updateTime = Instant.parse("2024-06-14T13:33:00Z");
        stateStore.fixFileUpdateTime(updateTime);
        FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(partitions, updateTime);
        FileReference inputFile = factory.rootFile("input.parquet", 123L);
        FileReference outputFile = factory.rootFile("output.parquet", 123L);
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(List.of("input.parquet"))
                .outputFile("output.parquet")
                .partitionId("root")
                .build();
        Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
        RecordsProcessedSummary summary = summary(Instant.parse("2024-06-14T15:35:00Z"), Duration.ofMinutes(2), 123, 123);
        CompactionJobCommitRequest commitRequest = new CompactionJobCommitRequest(job, "test-task", summary);

        stateStoreByTableId.put("test-table", stateStore);
        stateStore.addFile(inputFile);
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(
                "test-job", "root", List.of("input.parquet"))));
        compactionJobStatusStore.jobCreated(job, createdTime);
        compactionJobStatusStore.jobStarted(job, summary.getStartTime(), "test-task");

        // When
        committer().apply(StateStoreCommitRequest.forCompactionJob(commitRequest));

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
        assertThat(compactionJobStatusStore.getJob("test-job"))
                .contains(jobStatusFrom(records()
                        .fromUpdates(forJobOnTask("test-job", null,
                                CompactionJobCreatedStatus.from(job, createdTime)))
                        .fromUpdates(forJobOnTask("test-job", "test-task",
                                startedCompactionStatus(summary.getStartTime()),
                                finishedCompactionStatus(summary)))));
    }

    private StateStoreCommitter committer() {
        return new StateStoreCommitter(new CompactionJobCommitter(compactionJobStatusStore, stateStoreByTableId::get));
    }
}
