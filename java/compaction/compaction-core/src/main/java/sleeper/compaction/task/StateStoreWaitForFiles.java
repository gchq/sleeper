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
package sleeper.compaction.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.task.CompactionTask.WaitForFileAssignment;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.GetStateStoreByTableId;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class StateStoreWaitForFiles implements WaitForFileAssignment {

    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreWaitForFiles.class);

    public static final int JOB_ASSIGNMENT_WAIT_ATTEMPTS = 10;
    public static final WaitRange JOB_ASSIGNMENT_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(4, 60);
    public static final PollWithRetries JOB_ASSIGNMENT_THROTTLING_RETRIES = PollWithRetries.intervalAndPollingTimeout(Duration.ofMinutes(1), Duration.ofMinutes(10));

    private final int jobAssignmentWaitAttempts;
    private final ExponentialBackoffWithJitter jobAssignmentWaitBackoff;
    private final PollWithRetries throttlingRetriesConfig;
    private final GetStateStoreByTableId stateStoreProvider;

    public StateStoreWaitForFiles(GetStateStoreByTableId stateStoreProvider) {
        this(JOB_ASSIGNMENT_WAIT_ATTEMPTS, new ExponentialBackoffWithJitter(JOB_ASSIGNMENT_WAIT_RANGE),
                JOB_ASSIGNMENT_THROTTLING_RETRIES, stateStoreProvider);
    }

    public StateStoreWaitForFiles(
            int jobAssignmentWaitAttempts, ExponentialBackoffWithJitter jobAssignmentWaitBackoff,
            PollWithRetries throttlingRetriesConfig, GetStateStoreByTableId stateStoreProvider) {
        this.jobAssignmentWaitAttempts = jobAssignmentWaitAttempts;
        this.jobAssignmentWaitBackoff = jobAssignmentWaitBackoff;
        this.throttlingRetriesConfig = throttlingRetriesConfig;
        this.stateStoreProvider = stateStoreProvider;
    }

    @Override
    public void wait(CompactionJob job) throws InterruptedException {
        Instant startTime = Instant.now();
        LOGGER.info("Waiting for {} file{} to be assigned to compaction job {}",
                job.getInputFiles().size(), job.getInputFiles().size() > 1 ? "s" : "", job.getId());
        StateStore stateStore = stateStoreProvider.getByTableId(job.getTableId());
        // If transaction log DynamoDB table is scaling up, wait with retries limited over all assignment wait attempts for this job
        PollWithRetries throttlingRetries = throttlingRetriesConfig.toBuilder()
                .trackMaxRetriesAcrossInvocations()
                .build();
        for (int attempt = 1; attempt <= jobAssignmentWaitAttempts; attempt++) {
            jobAssignmentWaitBackoff.waitBeforeAttempt(attempt);
            if (allFilesAssignedToJob(throttlingRetries, stateStore, job)) {
                LOGGER.info("All files are assigned to job. Checked {} time{} and took {}",
                        attempt, attempt > 1 ? "s" : "", LoggedDuration.withFullOutput(startTime, Instant.now()));
                return;
            }
        }
        LOGGER.info("Reached maximum attempts of {} for checking if files are assigned to job", jobAssignmentWaitAttempts);
        throw new TimedOutWaitingForFileAssignmentsException();
    }

    private boolean allFilesAssignedToJob(PollWithRetries throttlingRetries, StateStore stateStore, CompactionJob job) throws InterruptedException {
        AtomicReference<List<FileReference>> files = new AtomicReference<>();
        DynamoDBUtils.retryOnThrottlingException(throttlingRetries, () -> {
            try {
                files.set(stateStore.getFileReferences());
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        });
        return files.get().stream()
                .filter(file -> isInputFileForJob(file, job))
                .allMatch(file -> job.getId().equals(file.getJobId()));
    }

    private static boolean isInputFileForJob(FileReference file, CompactionJob job) {
        return job.getInputFiles().contains(file.getFilename()) &&
                job.getPartitionId().equals(file.getPartitionId());
    }

}
