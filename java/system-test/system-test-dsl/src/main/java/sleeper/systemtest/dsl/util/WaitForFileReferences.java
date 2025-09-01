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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.PollWithRetries;

import java.util.List;

public class WaitForFileReferences {
    public static final Logger LOGGER = LoggerFactory.getLogger(WaitForFileReferences.class);

    private WaitForFileReferences() {
    }

    public static void waitForTotalFileReferences(int expectedFileReferences, StateStore stateStore, PollWithRetries poll) {
        try {
            poll.pollUntil("file references are added", () -> {
                List<FileReference> fileReferences = stateStore.getFileReferences();
                LOGGER.info("Found {} file references, waiting for expected {}", fileReferences.size(), expectedFileReferences);
                if (fileReferences.size() > expectedFileReferences) {
                    throw new RuntimeException("Was waiting for " + expectedFileReferences + " file references, overshot and found " + fileReferences.size());
                }
                return fileReferences.size() == expectedFileReferences;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
