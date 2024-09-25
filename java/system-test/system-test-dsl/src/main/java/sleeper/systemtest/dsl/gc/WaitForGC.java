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

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class WaitForGC {

    private WaitForGC() {
    }

    public static void waitUntilNoUnreferencedFiles(SystemTestInstanceContext instance, PollWithRetries poll) {
        Map<String, TableProperties> tablesById = instance.streamTableProperties()
                .collect(toMap(table -> table.get(TABLE_ID), table -> table));
        try {
            poll.pollUntil("no unreferenced files are present", () -> {
                List<String> emptyTableIds = tablesById.values().stream()
                        .filter(table -> hasNoUnreferencedFiles(instance.getStateStore(table)))
                        .map(table -> table.get(TABLE_ID))
                        .collect(toUnmodifiableList());
                emptyTableIds.forEach(tablesById::remove);
                return tablesById.isEmpty();
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static boolean hasNoUnreferencedFiles(StateStore stateStore) {
        try {
            return stateStore.getReadyForGCFilenamesBefore(Instant.now().plus(Duration.ofDays(1)))
                    .findAny().isEmpty();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

}
