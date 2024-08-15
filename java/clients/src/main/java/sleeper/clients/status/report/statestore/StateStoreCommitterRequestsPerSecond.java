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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.averagingDouble;

public class StateStoreCommitterRequestsPerSecond {

    private final double averageRequestsPerSecondInRuns;
    private final double averageRequestsPerSecondOverall;

    private StateStoreCommitterRequestsPerSecond(double averageRequestsPerSecondInRuns, double averageRequestsPerSecondOverall) {
        this.averageRequestsPerSecondInRuns = averageRequestsPerSecondInRuns;
        this.averageRequestsPerSecondOverall = averageRequestsPerSecondOverall;
    }

    public static StateStoreCommitterRequestsPerSecond fromRuns(List<StateStoreCommitterRun> runs) {
        return averageRequestsPerSecondInRunsAndOverall(
                computeAverageRequestsPerSecondInRuns(runs),
                computeAverageRequestsPerSecondOverall(runs));
    }

    public static Map<String, StateStoreCommitterRequestsPerSecond> byTableIdFromRuns(List<StateStoreCommitterRun> runs) {
        Map<String, List<StateStoreCommitterRun>> runsByTableId = StateStoreCommitterRuns.indexRunsByTableId(runs);
        Map<String, StateStoreCommitterRequestsPerSecond> byTableId = new HashMap<>();
        runsByTableId.forEach((tableId, tableRuns) -> byTableId.put(tableId, fromRuns(tableRuns)));
        return byTableId;
    }

    public static StateStoreCommitterRequestsPerSecond averageRequestsPerSecondInRunsAndOverall(double inRuns, double overall) {
        return new StateStoreCommitterRequestsPerSecond(inRuns, overall);
    }

    public double getAverageRequestsPerSecondInRuns() {
        return averageRequestsPerSecondInRuns;
    }

    public double getAverageRequestsPerSecondOverall() {
        return averageRequestsPerSecondOverall;
    }

    @Override
    public int hashCode() {
        return Objects.hash(averageRequestsPerSecondInRuns, averageRequestsPerSecondOverall);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitterRequestsPerSecond)) {
            return false;
        }
        StateStoreCommitterRequestsPerSecond other = (StateStoreCommitterRequestsPerSecond) obj;
        return Double.doubleToLongBits(averageRequestsPerSecondInRuns) == Double.doubleToLongBits(other.averageRequestsPerSecondInRuns)
                && Double.doubleToLongBits(averageRequestsPerSecondOverall) == Double.doubleToLongBits(other.averageRequestsPerSecondOverall);
    }

    @Override
    public String toString() {
        return "StateStoreCommitterReport{averageRequestsPerSecondInRuns=" + averageRequestsPerSecondInRuns + ", averageRequestsPerSecondOverall=" + averageRequestsPerSecondOverall + "}";
    }

    private static double computeAverageRequestsPerSecondInRuns(List<StateStoreCommitterRun> runs) {
        return runs.stream()
                .filter(run -> !run.getCommits().isEmpty())
                .collect(averagingDouble(StateStoreCommitterRun::computeRequestsPerSecond));
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
                if (lastLogEntry == null || entry.getTimeInCommitter().isAfter(lastLogEntry.getTimeInCommitter())) {
                    lastLogEntry = entry;
                }
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
