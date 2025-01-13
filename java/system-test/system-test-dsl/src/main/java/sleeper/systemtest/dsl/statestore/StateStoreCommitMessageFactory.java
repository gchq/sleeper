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
package sleeper.systemtest.dsl.statestore;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.core.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequestSerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequestSerDe;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequestSerDe;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class StateStoreCommitMessageFactory {

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;

    public StateStoreCommitMessageFactory(InstanceProperties instanceProperties, TableProperties tableProperties) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
    }

    public StateStoreCommitMessage addFiles(List<FileReference> files) {
        return ingest(builder -> builder.fileReferences(files));
    }

    public StateStoreCommitMessage addFileWithJob(FileReference file) {
        return addFilesWithJob(List.of(file));
    }

    public StateStoreCommitMessage addFilesWithJob(List<FileReference> files) {
        String jobId = UUID.randomUUID().toString();
        IngestJob job = IngestJob.builder()
                .id(jobId)
                .tableId(tableId())
                .files(List.of("test-file-" + jobId + ".parquet"))
                .build();
        return ingest(builder -> builder
                .fileReferences(files)
                .ingestJob(job)
                .taskId(jobId)
                .jobRunId(jobId)
                .writtenTime(Instant.now()));
    }

    public StateStoreCommitMessage assignJobOnPartitionToFiles(String jobId, String partitionId, List<String> filenames) {
        CompactionJobIdAssignmentCommitRequest request = CompactionJobIdAssignmentCommitRequest.tableRequests(tableId(),
                List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(jobId, partitionId, filenames)));
        return StateStoreCommitMessage.tableIdAndBody(tableId(),
                new CompactionJobIdAssignmentCommitRequestSerDe().toJson(request));
    }

    public StateStoreCommitMessage commitCompactionForPartitionOnTaskInRun(
            String jobId, String partitionId, List<String> filenames, String taskId, String jobRunId, JobRunSummary recordsProcessed) {
        CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
        CompactionJob job = factory.createCompactionJobWithFilenames(jobId, filenames, partitionId);
        CompactionJobCommitRequest request = new CompactionJobCommitRequest(job, taskId, jobRunId, recordsProcessed);
        return StateStoreCommitMessage.tableIdAndBody(tableId(),
                new CompactionJobCommitRequestSerDe().toJson(request));
    }

    public StateStoreCommitMessage filesDeleted(List<String> filenames) {
        GarbageCollectionCommitRequest request = new GarbageCollectionCommitRequest(tableId(), filenames);
        return StateStoreCommitMessage.tableIdAndBody(tableId(),
                new GarbageCollectionCommitRequestSerDe().toJson(request));
    }

    private StateStoreCommitMessage ingest(Consumer<IngestAddFilesCommitRequest.Builder> config) {
        IngestAddFilesCommitRequest.Builder builder = IngestAddFilesCommitRequest.builder().tableId(tableId());
        config.accept(builder);
        return StateStoreCommitMessage.tableIdAndBody(tableId(),
                new IngestAddFilesCommitRequestSerDe().toJson(builder.build()));
    }

    private String tableId() {
        return tableProperties.get(TABLE_ID);
    }

}
