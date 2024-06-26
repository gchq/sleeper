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

package sleeper.systemtest.dsl.compaction;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.core.util.PollWithRetries;

import java.time.Duration;

public interface CompactionDriver {

    CompactionJobStatusStore getJobStatusStore();

    void triggerCreateJobs();

    void forceCreateJobs();

    void invokeTasks(int expectedTasks, PollWithRetries poll);

    default void invokeTasks(int expectedTasks) {
        invokeTasks(expectedTasks, PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
    }

    void forceStartTasks(int numberOfTasks, PollWithRetries poll);

    default void forceStartTasks(int expectedTasks) {
        forceStartTasks(expectedTasks, PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
    }

    void scaleToZero();
}
