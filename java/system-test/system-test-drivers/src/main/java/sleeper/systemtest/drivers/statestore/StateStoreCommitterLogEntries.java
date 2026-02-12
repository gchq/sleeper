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
package sleeper.systemtest.drivers.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.report.statestore.StateStoreCommitSummary;
import sleeper.clients.report.statestore.StateStoreCommitterLogEntry;
import sleeper.clients.report.statestore.StateStoreCommitterRequestsPerSecond;
import sleeper.clients.report.statestore.StateStoreCommitterRun;
import sleeper.clients.report.statestore.StateStoreCommitterRuns;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogs;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.toMap;

public class StateStoreCommitterLogEntries implements StateStoreCommitterLogs {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitterLogEntries.class);
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
        LOGGER.info("Test1 - logs size: {}", logs.size());
        List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);
        LOGGER.info("Test2 - runs size: {}", runs.size());
        Map<String, List<StateStoreCommitterRun>> runsByTableId = StateStoreCommitterRuns.indexRunsByTableId(runs);
        LOGGER.info("Test3 - runs map size: {}", runsByTableId.size());
        return tableIds.stream()
                .collect(toMap(id -> id, tableId -> {
                    LOGGER.info("Test4 - tableId {}", tableId);
                    List<StateStoreCommitterRun> tableRuns = runsByTableId.getOrDefault(tableId, List.of());
                    LOGGER.info("Test5 - tableRuns {}", tableRuns.size());
                    long matchingCommits = tableRuns.stream()
                            .filter(run -> !run.getCommits().isEmpty())
                            .count();
                    LOGGER.info("Test6 - Matching Commits {}", matchingCommits);
                    return StateStoreCommitterRequestsPerSecond.computeAverageRequestsPerSecondInRuns(tableRuns);
                }));
    }

}
