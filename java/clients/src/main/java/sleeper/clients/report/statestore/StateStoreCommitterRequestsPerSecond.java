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
package sleeper.clients.report.statestore;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.averagingDouble;

/**
 * Computes the average speed of transaction commits in the state store committer. This averages the commits per second
 * in two separate ways.
 * <p>
 * With requests per second in runs, we recognise that each invocation of the state store committer has its own internal
 * clock, and different invocations can have clocks that are out of sync. This means it can be more accurate to compute
 * the commits per second in each invocation separately, before averaging the figure across the invocations.
 * <p>
 * With requests per second overall, we find the first and last time reported across all the invocations, take a total
 * amount of time based on that, and divide the total number of commits by that period. This is only accurate if the
 * clocks of the first and last time are very similar.
 */
public class StateStoreCommitterRequestsPerSecond {

    private final double averageRequestsPerSecondInRuns;
    private final double averageRequestsPerSecondOverall;

    private StateStoreCommitterRequestsPerSecond(double averageRequestsPerSecondInRuns, double averageRequestsPerSecondOverall) {
        this.averageRequestsPerSecondInRuns = averageRequestsPerSecondInRuns;
        this.averageRequestsPerSecondOverall = averageRequestsPerSecondOverall;
    }

    /**
     * Computes the average speed of transaction commits. This is based on log entries in a number of runs/invocations
     * of the state store committer.
     *
     * @param  runs the runs of the state store committer
     * @return      the statistics
     */
    public static StateStoreCommitterRequestsPerSecond fromRuns(List<StateStoreCommitterRun> runs) {
        return averageRequestsPerSecondInRunsAndOverall(
                computeAverageRequestsPerSecondInRuns(runs),
                computeAverageRequestsPerSecondOverall(runs));
    }

    /**
     * Computes the average speed of transaction commits for each Sleeper table. This is based on log entries in a
     * number of runs/invocations of the state store committer.
     *
     * @param  runs the runs of the state store committer
     * @return      a map from Sleeper table ID to the statistics
     */
    public static Map<String, StateStoreCommitterRequestsPerSecond> byTableIdFromRuns(List<StateStoreCommitterRun> runs) {
        Map<String, List<StateStoreCommitterRun>> runsByTableId = StateStoreCommitterRuns.indexRunsByTableId(runs);
        Map<String, StateStoreCommitterRequestsPerSecond> byTableId = new HashMap<>();
        runsByTableId.forEach((tableId, tableRuns) -> byTableId.put(tableId, fromRuns(tableRuns)));
        return byTableId;
    }

    /**
     * Creates an instance of this class with the given average speed. This takes the commits per second averaged in two
     * separate ways. Also see the class comment.
     *
     * @param  inRuns  the commits per second averaged within in each run of the state store committer
     * @param  overall the commits per second averaged across the whole period, assuming synchronized clocks
     * @return         the statistics
     */
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

    /**
     * Computes the average commits per second for each run/invocation of the state store committer, and averages across
     * these figures.
     *
     * @param  runs logs from each run of the state store committer
     * @return      the average commits per second
     */
    public static double computeAverageRequestsPerSecondInRuns(List<StateStoreCommitterRun> runs) {
        return runs.stream()
                .filter(run -> !run.getCommits().isEmpty())
                .collect(averagingDouble(StateStoreCommitterRun::computeRequestsPerSecond));
    }

    /**
     * Computes the average commits per second by finding the start and end time, and dividing the number of commits by
     * that period. This assumes that every run of the state store committer uses the same, synchronized clock, which
     * may not be true.
     *
     * @param  runs logs from each run of the state store committer
     * @return      the average commits per second
     */
    public static double computeAverageRequestsPerSecondOverall(List<StateStoreCommitterRun> runs) {
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
