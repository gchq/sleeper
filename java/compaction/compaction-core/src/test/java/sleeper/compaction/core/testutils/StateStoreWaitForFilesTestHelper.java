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
package sleeper.compaction.core.testutils;

import sleeper.compaction.core.task.StateStoreWaitForFiles;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobStatusStore;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.PollWithRetries;
import sleeper.core.util.ThreadSleep;

import java.time.Instant;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;

public class StateStoreWaitForFilesTestHelper {

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final ThreadSleep waiter;
    private final Supplier<Instant> timeSupplier;

    public StateStoreWaitForFilesTestHelper(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, CompactionJobStatusStore jobStatusStore,
            ThreadSleep waiter, Supplier<Instant> timeSupplier) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.jobStatusStore = jobStatusStore;
        this.waiter = waiter;
        this.timeSupplier = timeSupplier;
    }

    public StateStoreWaitForFiles withAttempts(int attempts) {
        return waitWithAttempts(attempts, noJitter(), PollWithRetries.noRetries());
    }

    public StateStoreWaitForFiles withAttemptsAndThrottlingRetries(
            int attempts, DoubleSupplier jitter) {
        return waitWithAttempts(attempts, jitter, StateStoreWaitForFiles.JOB_ASSIGNMENT_THROTTLING_RETRIES);
    }

    private StateStoreWaitForFiles waitWithAttempts(
            int attempts, DoubleSupplier jitter, PollWithRetries throttlingRetries) {
        return new StateStoreWaitForFiles(attempts,
                new ExponentialBackoffWithJitter(StateStoreWaitForFiles.JOB_ASSIGNMENT_WAIT_RANGE, jitter, waiter),
                throttlingRetries.toBuilder()
                        .sleepInInterval(waiter::waitForMillis)
                        .build(),
                tablePropertiesProvider, stateStoreProvider, jobStatusStore, timeSupplier);
    }
}
