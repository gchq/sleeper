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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.time.Duration;

public interface InvokeIngestTasksDriver {

    void invokeStandardIngestTasks(int expectedTasks, PollWithRetries poll);

    default void invokeStandardIngestTask() {
        invokeStandardIngestTasks(1,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
    }

    default void invokeStandardIngestTask(PollWithRetriesDriver pollDriver) {
        invokeStandardIngestTasks(1,
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
    }
}
