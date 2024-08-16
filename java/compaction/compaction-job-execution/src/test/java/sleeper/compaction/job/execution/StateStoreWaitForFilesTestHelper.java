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
package sleeper.compaction.job.execution;

import sleeper.compaction.task.StateStoreWaitForFiles;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.PollWithRetries;
import sleeper.statestore.StateStoreProvider;

import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noWaits;

public class StateStoreWaitForFilesTestHelper {
    private StateStoreWaitForFilesTestHelper() {
    }

    public static StateStoreWaitForFiles waitWithRetries(int retries, StateStoreProvider stateStoreProvider, TablePropertiesProvider tablePropertiesProvider) {
        return new StateStoreWaitForFiles(retries, backoffNoJitter(), PollWithRetries.noRetries(), stateStoreProvider.byTableId(tablePropertiesProvider));
    }

    private static ExponentialBackoffWithJitter backoffNoJitter() {
        return new ExponentialBackoffWithJitter(
                StateStoreWaitForFiles.JOB_ASSIGNMENT_WAIT_RANGE,
                noJitter(), noWaits());
    }
}
