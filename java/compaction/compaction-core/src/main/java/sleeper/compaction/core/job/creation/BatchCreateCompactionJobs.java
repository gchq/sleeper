/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.compaction.core.job.creation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.SplitIntoBatches;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_BATCH_SIZE;

public class BatchCreateCompactionJobs {
    public static final Logger LOGGER = LoggerFactory.getLogger(BatchCreateCompactionJobs.class);

    private final InstanceProperties instanceProperties;
    private final BatchJobsWriter batchJobsWriter;
    private final BatchMessageSender batchMessageSender;
    private final StateStoreCommitRequestSender stateStoreCommitSender;
    private final Supplier<String> batchIdSupplier;
    private final Supplier<Instant> timeSupplier;

    public BatchCreateCompactionJobs(
            InstanceProperties instanceProperties,
            BatchJobsWriter batchJobsWriter,
            BatchMessageSender batchMessageSender,
            StateStoreCommitRequestSender stateStoreCommitSender,
            Supplier<String> batchIdSupplier, Supplier<Instant> timeSupplier) {
        this.instanceProperties = instanceProperties;
        this.batchJobsWriter = batchJobsWriter;
        this.batchMessageSender = batchMessageSender;
        this.stateStoreCommitSender = stateStoreCommitSender;
        this.batchIdSupplier = batchIdSupplier;
        this.timeSupplier = timeSupplier;
    }

    public void createJobs(TableProperties tableProperties, StateStore stateStore, List<CompactionJob> jobs) throws IOException {
        AssignJobIdToFiles assignJobIdsToFiles;
        if (tableProperties.getBoolean(COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC)) {
            assignJobIdsToFiles = AssignJobIdToFiles.byQueue(stateStoreCommitSender);
        } else {
            assignJobIdsToFiles = AssignJobIdToFiles.synchronous(stateStore);
        }
        int sendBatchSize = tableProperties.getInt(COMPACTION_JOB_SEND_BATCH_SIZE);
        for (List<CompactionJob> batch : SplitIntoBatches.splitListIntoBatchesOf(sendBatchSize, jobs)) {
            batchCreateJobs(assignJobIdsToFiles, tableProperties, batch);
        }
    }

    private void batchCreateJobs(AssignJobIdToFiles assignJobIdToFiles, TableProperties tableProperties, List<CompactionJob> compactionJobs) throws IOException {
        TableStatus tableStatus = tableProperties.getStatus();
        CompactionJobDispatchRequest request = CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                tableProperties, batchIdSupplier.get(), timeSupplier.get());

        // Send batch of jobs to SQS (NB Send jobs to SQS before updating the job field of the files in the
        // StateStore so that if the send to SQS fails then the StateStore will not be updated and later another
        // job can be created for these files)
        LOGGER.debug("Writing batch of {} compaction jobs for table {} in data bucket at: {}", compactionJobs.size(), tableStatus, request.getBatchKey());
        Instant startTime = Instant.now();
        batchJobsWriter.writeJobs(instanceProperties.get(DATA_BUCKET), request.getBatchKey(), compactionJobs);
        LOGGER.debug("Sending compaction jobs batch, wrote jobs in {}",
                LoggedDuration.withShortOutput(startTime, Instant.now()));
        batchMessageSender.sendMessage(request);

        // Update the statuses of these files to record that a compaction job is in progress
        LOGGER.debug("Assigning input files for compaction jobs batch in table {}", tableStatus);
        assignJobIdToFiles.assignJobIds(compactionJobs.stream()
                .map(CompactionJob::createAssignJobIdRequest)
                .collect(Collectors.toList()), tableStatus);
        LOGGER.info("Created pending batch of {} compaction jobs for table {} in data bucket at: {}", compactionJobs.size(), tableStatus, request.getBatchKey());
    }

    @FunctionalInterface
    public interface BatchJobsWriter {
        void writeJobs(String bucketName, String key, List<CompactionJob> compactionJobs);
    }

    @FunctionalInterface
    public interface BatchMessageSender {
        void sendMessage(CompactionJobDispatchRequest dispatchRequest);
    }
}
