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

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.toUnmodifiableList;

public class StateStoreCommitterLogIndex {

    private final List<StateStoreCommitterRun> runs;
    private final double averageRequestsPerSecondInRuns;
    private final double averageRequestsPerSecondOverall;

    private StateStoreCommitterLogIndex(Builder builder) {
        runs = builder.buildRuns();
        averageRequestsPerSecondInRuns = runs.stream()
                .filter(run -> !run.getCommits().isEmpty())
                .collect(averagingDouble(StateStoreCommitterRun::computeRequestsPerSecond));
        averageRequestsPerSecondOverall = computeAverageRequestsPerSecondOverall(runs);
    }

    public static StateStoreCommitterLogIndex from(List<StateStoreCommitterLogEntry> logs) {
        Builder builder = new Builder();
        logs.forEach(builder::add);
        return builder.build();
    }

    public List<StateStoreCommitterRun> getRuns() {
        return runs;
    }

    public double getAverageRequestsPerSecondInRuns() {
        return averageRequestsPerSecondInRuns;
    }

    public double getAverageRequestsPerSecondOverall() {
        return averageRequestsPerSecondOverall;
    }

    private static class Builder {
        private final Map<String, List<StateStoreCommitterLogEntry>> entriesByLogStream = new LinkedHashMap<>();

        private void add(StateStoreCommitterLogEntry entry) {
            entriesByLogStream.computeIfAbsent(entry.getLogStream(), stream -> new ArrayList<>())
                    .add(entry);
        }

        private StateStoreCommitterLogIndex build() {
            return new StateStoreCommitterLogIndex(this);
        }

        private List<StateStoreCommitterRun> buildRuns() {
            return entriesByLogStream.values().stream()
                    .flatMap(entries -> StateStoreCommitterRun.splitIntoRuns(entries).stream())
                    .collect(toUnmodifiableList());
        }
    }

    private static double computeAverageRequestsPerSecondOverall(List<StateStoreCommitterRun> runs) {
        StateStoreCommitterLogEntry firstLogEntry = null;
        StateStoreCommitterLogEntry lastLogEntry = null;
        int numCommits = 0;
        for (StateStoreCommitterRun run : runs) {
            List<StateStoreCommitSummary> commits = run.getCommits();
            if (commits.isEmpty()) {
                continue;
            }
            numCommits += commits.size();
            for (StateStoreCommitterLogEntry entry : (Iterable<StateStoreCommitterLogEntry>) () -> run.entries().iterator()) {
                if (firstLogEntry == null) {
                    firstLogEntry = entry;
                }
                lastLogEntry = entry;
            }
        }
        if (firstLogEntry == null || lastLogEntry == null) {
            return 0.0;
        }
        Duration duration = Duration.between(firstLogEntry.getTimeInCommitter(), lastLogEntry.getTimeInCommitter());
        double seconds = duration.toMillis() / 1000.0;
        if (seconds <= 0.0) {
            return 0.0;
        }
        return numCommits / seconds;
    }

}
