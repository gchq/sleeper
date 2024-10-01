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
package sleeper.compaction.testutils;

import sleeper.compaction.task.StateStoreWaitForFiles;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;
import sleeper.core.util.PollWithRetries;

import java.util.function.DoubleSupplier;

import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noWaits;

public class StateStoreWaitForFilesTestHelper {
    private StateStoreWaitForFilesTestHelper() {
    }

    public static StateStoreWaitForFiles waitForFileAssignmentWithAttempts(
            int attempts, StateStoreProvider stateStoreProvider, TablePropertiesProvider tablePropertiesProvider) {
        return waitWithAttempts(attempts, noJitter(), noWaits(), PollWithRetries.noRetries(), tablePropertiesProvider, stateStoreProvider);
    }

    public static StateStoreWaitForFiles waitForFileAssignmentWithAttempts(
            int attempts, Waiter waiter,
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) {
        return waitWithAttempts(attempts, noJitter(), waiter, PollWithRetries.noRetries(), tablePropertiesProvider, stateStoreProvider);
    }

    public static StateStoreWaitForFiles waitForFileAssignmentWithAttemptsAndThrottlingRetries(
            int attempts, DoubleSupplier jitter, Waiter waiter,
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) {
        return waitWithAttempts(attempts, jitter, waiter, StateStoreWaitForFiles.JOB_ASSIGNMENT_THROTTLING_RETRIES, tablePropertiesProvider, stateStoreProvider);
    }

    public static Waiter withActionAfterWait(Waiter waiter, WaitAction action) throws Exception {
        return millis -> {
            try {
                action.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            waiter.waitForMillis(millis);
        };
    }

    private static StateStoreWaitForFiles waitWithAttempts(
            int attempts, DoubleSupplier jitter, Waiter waiter, PollWithRetries throttlingRetries,
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) {
        return new StateStoreWaitForFiles(attempts,
                new ExponentialBackoffWithJitter(StateStoreWaitForFiles.JOB_ASSIGNMENT_WAIT_RANGE, jitter, waiter),
                throttlingRetries.toBuilder()
                        .sleepInInterval(waiter::waitForMillis)
                        .build(),
                tablePropertiesProvider, stateStoreProvider);
    }

    public interface WaitAction {
        void run() throws Exception;
    }
}
