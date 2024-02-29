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
package sleeper.systemtest.dsl.gc;

import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;

import java.time.Duration;
import java.time.Instant;

public class WaitForGC {

    private WaitForGC() {
    }

    public static void waitUntilNoUnreferencedFiles(StateStore stateStore, PollWithRetries poll) {
        try {
            poll.pollUntil("no unreferenced files are present", () -> {
                try {
                    return stateStore.getReadyForGCFilenamesBefore(Instant.now().plus(Duration.ofDays(1)))
                            .findAny().isEmpty();
                } catch (StateStoreException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
