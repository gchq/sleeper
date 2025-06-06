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
package sleeper.compaction.core.task;

import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.PollWithRetries;
import sleeper.core.util.RetryOnDynamoDbThrottling;
import sleeper.core.util.ThreadSleep;

import java.time.Instant;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;
import static sleeper.core.util.RetryOnDynamoDbThrottlingTestHelper.noRetriesOnThrottling;
import static sleeper.core.util.RetryOnDynamoDbThrottlingTestHelper.retryOnThrottlingException;

public class StateStoreWaitForFilesTestHelper {

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker jobTracker;
    private final ThreadSleep waiter;
    private final Supplier<Instant> timeSupplier;

    public StateStoreWaitForFilesTestHelper(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, CompactionJobTracker jobTracker,
            ThreadSleep waiter, Supplier<Instant> timeSupplier) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.jobTracker = jobTracker;
        this.waiter = waiter;
        this.timeSupplier = timeSupplier;
    }

    public StateStoreWaitForFiles withAttempts(int attempts) {
        return withAttempts(attempts, noJitter());
    }

    public StateStoreWaitForFiles withAttempts(
            int attempts, DoubleSupplier jitter) {
        return withAttempts(attempts, jitter, noRetriesOnThrottling(), PollWithRetries.noRetries());
    }

    public StateStoreWaitForFiles withThrottlingRetriesOnException(RuntimeException throttlingException) {
        return withAttempts(1, noJitter(), retryOnThrottlingException(throttlingException), StateStoreWaitForFiles.JOB_ASSIGNMENT_THROTTLING_RETRIES);
    }

    private StateStoreWaitForFiles withAttempts(
            int attempts, DoubleSupplier jitter, RetryOnDynamoDbThrottling retryOnThrottling, PollWithRetries throttlingRetries) {
        return new StateStoreWaitForFiles(attempts,
                new ExponentialBackoffWithJitter(StateStoreWaitForFiles.JOB_ASSIGNMENT_WAIT_RANGE, jitter, waiter),
                retryOnThrottling,
                throttlingRetries.toBuilder()
                        .sleepInInterval(waiter::waitForMillis)
                        .build(),
                tablePropertiesProvider, stateStoreProvider, jobTracker, timeSupplier);
    }
}
