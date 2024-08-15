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
package sleeper.clients.status.report.statestore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toUnmodifiableList;

public class StateStoreCommitterLogIndex {

    private final List<StateStoreCommitterRun> runs;

    private StateStoreCommitterLogIndex(List<StateStoreCommitterRun> runs) {
        this.runs = runs;
    }

    public static StateStoreCommitterLogIndex from(List<StateStoreCommitterLogEntry> logs) {
        Builder builder = new Builder();
        logs.forEach(builder::add);
        return builder.build();
    }

    public List<StateStoreCommitterRun> getRuns() {
        return runs;
    }

    private static class Builder {
        private final Map<String, List<StateStoreCommitterLogEntry>> entriesByLogStream = new LinkedHashMap<>();

        private void add(StateStoreCommitterLogEntry entry) {
            entriesByLogStream.computeIfAbsent(entry.getLogStream(), stream -> new ArrayList<>())
                    .add(entry);
        }

        private StateStoreCommitterLogIndex build() {
            List<StateStoreCommitterRun> runs = entriesByLogStream.values().stream()
                    .flatMap(entries -> StateStoreCommitterRun.splitIntoRuns(entries).stream())
                    .collect(toUnmodifiableList());
            return new StateStoreCommitterLogIndex(runs);
        }
    }
}
