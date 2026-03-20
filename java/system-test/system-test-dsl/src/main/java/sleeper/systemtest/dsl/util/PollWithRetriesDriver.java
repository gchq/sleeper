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
package sleeper.systemtest.dsl.util;

import sleeper.core.util.PollWithRetries;

import java.time.Duration;

@FunctionalInterface
public interface PollWithRetriesDriver {

    PollWithRetries poll(PollWithRetries config);

    default PollWithRetries pollWithIntervalAndTimeout(Duration pollInterval, Duration timeout) {
        return poll(PollWithRetries.intervalAndPollingTimeout(pollInterval, timeout));
    }

    static PollWithRetriesDriver realWaits() {
        return poll -> poll;
    }

    static PollWithRetriesDriver noWaits() {
        return poll -> poll.toBuilder()
                .sleepInInterval(millis -> { // Do not really wait
                }).build();
    }

}
