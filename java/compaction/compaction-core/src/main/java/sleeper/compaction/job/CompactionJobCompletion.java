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
package sleeper.compaction.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;

public class CompactionJobCompletion {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCompletion.class);

    public static final int JOB_ASSIGNMENT_WAIT_ATTEMPTS = 10;
    public static final WaitRange JOB_ASSIGNMENT_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(2, 60);

    private final CompactionJobStatusStore statusStore;
    private final GetStateStore stateStoreProvider;
    private final int jobAssignmentWaitAttempts;
    private final ExponentialBackoffWithJitter jobAssignmentWaitBackoff;

    public CompactionJobCompletion(
            CompactionJobStatusStore statusStore, GetStateStore stateStoreProvider) {
        this(statusStore, stateStoreProvider, JOB_ASSIGNMENT_WAIT_ATTEMPTS,
                new ExponentialBackoffWithJitter(JOB_ASSIGNMENT_WAIT_RANGE));
    }

    public CompactionJobCompletion(
            CompactionJobStatusStore statusStore, GetStateStore stateStoreProvider,
            int jobAssignmentWaitAttempts, ExponentialBackoffWithJitter jobAssignmentWaitBackoff) {
        this.statusStore = statusStore;
        this.stateStoreProvider = stateStoreProvider;
        this.jobAssignmentWaitAttempts = jobAssignmentWaitAttempts;
        this.jobAssignmentWaitBackoff = jobAssignmentWaitBackoff;
    }

    public void apply(CompactionJobCompletionRequest request) throws StateStoreException, InterruptedException {
        CompactionJob job = request.getJob();
        updateStateStoreSuccess(job, request.getRecordsWritten());
        statusStore.jobFinished(job, request.buildRecordsProcessedSummary(), request.getTaskId());
    }

    private void updateStateStoreSuccess(CompactionJob job, long recordsWritten) throws StateStoreException, InterruptedException {
        StateStore stateStore = stateStoreProvider.getByTableId(job.getTableId());
        FileReference fileReference = FileReference.builder()
                .filename(job.getOutputFile())
                .partitionId(job.getPartitionId())
                .numberOfRecords(recordsWritten)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();

        // Compaction jobs are sent for execution before updating the state store to assign the input files to the job.
        // Sometimes the compaction can finish before the job assignment is finished. We wait for the job assignment
        // rather than immediately failing the job run.
        FileReferenceNotAssignedToJobException failure = null;
        for (int attempts = 0; attempts < jobAssignmentWaitAttempts; attempts++) {
            jobAssignmentWaitBackoff.waitBeforeAttempt(attempts);
            try {
                stateStore.atomicallyReplaceFileReferencesWithNewOne(job.getId(), job.getPartitionId(), job.getInputFiles(), fileReference);
                LOGGER.debug("Updated file references in state store");
                return;
            } catch (FileReferenceNotAssignedToJobException e) {
                LOGGER.warn("Job not yet assigned to input files on attempt {} of {}: {}",
                        attempts + 1, jobAssignmentWaitAttempts, e.getMessage());
                failure = e;
            }
        }
        throw new TimedOutWaitingForFileAssignmentsException(failure);
    }

    @FunctionalInterface
    public interface GetStateStore {
        StateStore getByTableId(String tableId);
    }
}
