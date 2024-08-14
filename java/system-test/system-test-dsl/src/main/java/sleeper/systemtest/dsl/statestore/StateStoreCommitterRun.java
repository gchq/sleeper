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
package sleeper.systemtest.dsl.statestore;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;

public class StateStoreCommitterRun {

    private final Instant startTime;
    private final Instant finishTime;
    private final List<StateStoreCommitSummary> commits;

    public StateStoreCommitterRun(Instant startTime, Instant finishTime, List<StateStoreCommitSummary> commits) {
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.commits = commits;
    }

    public static void decrementWaitForNumCommits(List<StateStoreCommitterRun> runs, Map<String, Integer> waitForNumCommitsByTableId) {
        Map<String, Integer> numCommitsByTableId = runs.stream()
                .flatMap(run -> run.getCommits().stream())
                .collect(groupingBy(StateStoreCommitSummary::getTableId, summingInt(commit -> 1)));
        numCommitsByTableId.forEach((tableId, numCommits) -> {
            waitForNumCommitsByTableId.compute(tableId, (id, count) -> {
                if (count == null) {
                    return null;
                } else if (numCommits >= count) {
                    return null;
                } else {
                    return count - numCommits;
                }
            });
        });
    }

    public static Optional<Instant> getLastTime(List<StateStoreCommitterRun> runs) {
        return runs.stream()
                .flatMap(run -> run.getLastTime().stream())
                .max(Comparator.naturalOrder());
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    public Optional<Instant> getLastTime() {
        if (finishTime != null) {
            return Optional.of(finishTime);
        }
        Optional<Instant> commitTime = commits.stream()
                .map(StateStoreCommitSummary::getFinishTime)
                .max(Comparator.naturalOrder());
        if (commitTime.isPresent()) {
            return commitTime;
        } else {
            return Optional.ofNullable(startTime);
        }
    }

    public List<StateStoreCommitSummary> getCommits() {
        return commits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, finishTime, commits);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitterRun)) {
            return false;
        }
        StateStoreCommitterRun other = (StateStoreCommitterRun) obj;
        return Objects.equals(startTime, other.startTime) && Objects.equals(finishTime, other.finishTime) && Objects.equals(commits, other.commits);
    }

    @Override
    public String toString() {
        return "StateStoreCommitterRun{startTime=" + startTime + ", finishTime=" + finishTime + ", commits=" + commits + "}";
    }
}
