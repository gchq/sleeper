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
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.tracker.job.run.JobRunSummary;

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

    public StateStoreCommitRequest addFiles(List<FileReference> files) {
        return ingest(builder -> builder.files(AllReferencesToAFile.newFilesWithReferences(files)));
    }

    public StateStoreCommitRequest addFileWithJob(FileReference file) {
        return addFilesWithJob(List.of(file));
    }

    public StateStoreCommitRequest addFilesWithJob(List<FileReference> files) {
        String jobId = UUID.randomUUID().toString();
        return ingest(builder -> builder
                .files(AllReferencesToAFile.newFilesWithReferences(files))
                .jobId(jobId)
                .taskId(jobId)
                .jobRunId(jobId)
                .writtenTime(Instant.now()));
    }

    public StateStoreCommitRequest assignJobOnPartitionToFiles(String jobId, String partitionId, List<String> filenames) {
        return message(new AssignJobIdsTransaction(
                List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(jobId, partitionId, filenames))));
    }

    public StateStoreCommitRequest commitCompactionForPartitionOnTaskInRun(
            String jobId, String partitionId, List<String> filenames, String taskId, String jobRunId, JobRunSummary recordsProcessed) {
        CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
        CompactionJob job = factory.createCompactionJobWithFilenames(jobId, filenames, partitionId);
        return message(new ReplaceFileReferencesTransaction(List.of(
                job.replaceFileReferencesRequestBuilder(recordsProcessed.getRecordsProcessed().getRecordsWritten())
                        .taskId(taskId)
                        .jobRunId(jobRunId)
                        .build())));
    }

    public StateStoreCommitRequest filesDeleted(List<String> filenames) {
        return message(new DeleteFilesTransaction(filenames));
    }

    private StateStoreCommitRequest ingest(Consumer<AddFilesTransaction.Builder> config) {
        AddFilesTransaction.Builder builder = AddFilesTransaction.builder();
        config.accept(builder);
        return message(builder.build());
    }

    private StateStoreCommitRequest message(StateStoreTransaction<?> transaction) {
        return StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), transaction);
    }

}
