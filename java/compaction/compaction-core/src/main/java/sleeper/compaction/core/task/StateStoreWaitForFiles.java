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
package sleeper.compaction.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

public class StateStoreWaitForFiles {

    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreWaitForFiles.class);

    public static final int JOB_ASSIGNMENT_WAIT_ATTEMPTS = 10;
    public static final WaitRange JOB_ASSIGNMENT_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(4, 60);
    public static final PollWithRetries JOB_ASSIGNMENT_THROTTLING_RETRIES = PollWithRetries.intervalAndPollingTimeout(Duration.ofMinutes(1), Duration.ofMinutes(10));

    private final int jobAssignmentWaitAttempts;
    private final ExponentialBackoffWithJitter jobAssignmentWaitBackoff;
    private final PollWithRetries throttlingRetriesConfig;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker jobTracker;
    private final Supplier<Instant> timeSupplier;

    public StateStoreWaitForFiles(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            CompactionJobTracker jobTracker) {
        this(JOB_ASSIGNMENT_WAIT_ATTEMPTS, new ExponentialBackoffWithJitter(JOB_ASSIGNMENT_WAIT_RANGE),
                JOB_ASSIGNMENT_THROTTLING_RETRIES, tablePropertiesProvider, stateStoreProvider, jobTracker, Instant::now);
    }

    public StateStoreWaitForFiles(
            int jobAssignmentWaitAttempts,
            ExponentialBackoffWithJitter jobAssignmentWaitBackoff,
            PollWithRetries throttlingRetriesConfig,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            CompactionJobTracker jobTracker,
            Supplier<Instant> timeSupplier) {
        this.jobAssignmentWaitAttempts = jobAssignmentWaitAttempts;
        this.jobAssignmentWaitBackoff = jobAssignmentWaitBackoff;
        this.throttlingRetriesConfig = throttlingRetriesConfig;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.jobTracker = jobTracker;
        this.timeSupplier = timeSupplier;
    }

    public void wait(CompactionJob job, String taskId, String jobRunId) throws InterruptedException {
        Instant startTime = timeSupplier.get();
        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        LOGGER.info("Waiting for {} file{} to be assigned to compaction job {} for table {}",
                job.getInputFiles().size(), job.getInputFiles().size() > 1 ? "s" : "", job.getId(), tableProperties.getStatus());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        // If transaction log DynamoDB table is scaling up, wait with retries limited over all assignment wait attempts for this job
        PollWithRetries throttlingRetries = throttlingRetriesConfig.toBuilder()
                .trackMaxRetriesAcrossInvocations()
                .build();
        for (int attempt = 1; attempt <= jobAssignmentWaitAttempts; attempt++) {
            jobAssignmentWaitBackoff.waitBeforeAttempt(attempt);
            if (allFilesAssignedToJob(throttlingRetries, stateStore, job, taskId, jobRunId, startTime)) {
                LOGGER.info("All files are assigned to job. Checked {} time{} and took {}",
                        attempt, attempt > 1 ? "s" : "", LoggedDuration.withFullOutput(startTime, Instant.now()));
                return;
            }
        }
        LOGGER.info("Reached maximum attempts of {} for checking if files are assigned to job", jobAssignmentWaitAttempts);
        TimedOutWaitingForFileAssignmentsException e = new TimedOutWaitingForFileAssignmentsException();
        reportFailure(job, taskId, jobRunId, startTime, e);
        throw e;
    }

    private boolean allFilesAssignedToJob(
            PollWithRetries throttlingRetries, StateStore stateStore, CompactionJob job,
            String taskId, String jobRunId, Instant startTime) throws InterruptedException {
        ResultTracker result = new ResultTracker();
        try {
            DynamoDBUtils.retryOnThrottlingException(throttlingRetries, () -> {
                result.set(stateStore.isAssigned(List.of(job.createInputFileAssignmentsCheck())));
            });
        } catch (RuntimeException e) {
            reportFailure(job, taskId, jobRunId, startTime, e);
            throw e;
        }
        return result.get();
    }

    private void reportFailure(CompactionJob job, String taskId, String jobRunId, Instant startTime, Exception e) {
        Instant finishTime = timeSupplier.get();
        jobTracker.jobStarted(job.startedEventBuilder(startTime).taskId(taskId).jobRunId(jobRunId).build());
        jobTracker.jobFailed(job.failedEventBuilder(new JobRunTime(startTime, finishTime))
                .failure(e).taskId(taskId).jobRunId(jobRunId).build());
    }

    private static class ResultTracker {
        private boolean allFilesAssigned;

        void set(boolean allFilesAssigned) {
            this.allFilesAssigned = allFilesAssigned;
        }

        boolean get() {
            return allFilesAssigned;
        }
    }

}
