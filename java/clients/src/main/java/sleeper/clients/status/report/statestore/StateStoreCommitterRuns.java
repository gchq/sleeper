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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;

public class StateStoreCommitterRuns {

    private StateStoreCommitterRuns() {
    }

    public static List<StateStoreCommitterRun> findRunsByLogStream(List<StateStoreCommitterLogEntry> logs) {
        BuilderByLogStream builder = new BuilderByLogStream();
        logs.forEach(builder::add);
        return builder.buildRuns();
    }

    public static Map<String, List<StateStoreCommitterRun>> indexRunsByTableId(List<StateStoreCommitterRun> runs) {
        Map<String, List<StateStoreCommitterRun>> runsByTableId = new HashMap<>();
        for (StateStoreCommitterRun run : runs) {
            Set<String> tableIds = run.getCommits().stream()
                    .map(StateStoreCommitSummary::getTableId)
                    .collect(toSet());
            for (String tableId : tableIds) {
                runsByTableId.computeIfAbsent(tableId, id -> new ArrayList<>())
                        .add(run);
            }
        }
        return runsByTableId;
    }

    private static class BuilderByLogStream {
        private final Map<String, List<StateStoreCommitterLogEntry>> entriesByLogStream = new LinkedHashMap<>();

        private void add(StateStoreCommitterLogEntry entry) {
            entriesByLogStream.computeIfAbsent(entry.getLogStream(), stream -> new ArrayList<>())
                    .add(entry);
        }

        private List<StateStoreCommitterRun> buildRuns() {
            return entriesByLogStream.values().stream()
                    .flatMap(entries -> splitIntoRuns(entries).stream())
                    .collect(toUnmodifiableList());
        }
    }

    private static List<StateStoreCommitterRun> splitIntoRuns(List<StateStoreCommitterLogEntry> logs) {
        List<StateStoreCommitterRun> runs = new ArrayList<>();
        StateStoreCommitterRun.Builder builder = null;
        for (StateStoreCommitterLogEntry entry : logs) {
            if (entry instanceof StateStoreCommitterRunStarted) {
                if (builder != null) {
                    runs.add(builder.build());
                }
                builder = newRun(entry).start((StateStoreCommitterRunStarted) entry);
            } else if (entry instanceof StateStoreCommitSummary) {
                if (builder == null) {
                    builder = newRun(entry);
                }
                builder.commit((StateStoreCommitSummary) entry);
            } else if (entry instanceof StateStoreCommitterRunFinished) {
                if (builder == null) {
                    builder = newRun(entry);
                }
                runs.add(builder.finish((StateStoreCommitterRunFinished) entry).build());
                builder = null;
            }
        }
        if (builder != null) {
            runs.add(builder.build());
        }
        return runs;
    }

    private static StateStoreCommitterRun.Builder newRun(StateStoreCommitterLogEntry entry) {
        return StateStoreCommitterRun.builder().logStream(entry.getLogStream());
    }
}
