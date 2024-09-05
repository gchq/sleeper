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
package sleeper.systemtest.drivers.statestore;

import sleeper.clients.status.report.statestore.StateStoreCommitSummary;
import sleeper.clients.status.report.statestore.StateStoreCommitterLogEntry;
import sleeper.clients.status.report.statestore.StateStoreCommitterRequestsPerSecond;
import sleeper.clients.status.report.statestore.StateStoreCommitterRun;
import sleeper.clients.status.report.statestore.StateStoreCommitterRuns;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogs;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.toMap;

public class StateStoreCommitterLogEntries implements StateStoreCommitterLogs {

    private final List<StateStoreCommitterLogEntry> logs;

    public StateStoreCommitterLogEntries(List<StateStoreCommitterLogEntry> logs) {
        this.logs = logs;
    }

    @Override
    public Map<String, Integer> countNumCommitsByTableId(Set<String> tableIds) {
        return logs.stream()
                .filter(entry -> entry instanceof StateStoreCommitSummary)
                .map(entry -> (StateStoreCommitSummary) entry)
                .filter(commit -> tableIds.contains(commit.getTableId()))
                .collect(groupingBy(StateStoreCommitSummary::getTableId, summingInt(commit -> 1)));
    }

    @Override
    public Map<String, Double> computeOverallCommitsPerSecondByTableId(Set<String> tableIds) {
        List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);
        Map<String, List<StateStoreCommitterRun>> runsByTableId = StateStoreCommitterRuns.indexRunsByTableId(runs);
        return tableIds.stream()
                .collect(toMap(id -> id, tableId -> {
                    List<StateStoreCommitterRun> tableRuns = runsByTableId.getOrDefault(tableId, List.of());
                    return StateStoreCommitterRequestsPerSecond.computeAverageRequestsPerSecondOverall(tableRuns);
                }));
    }

}
