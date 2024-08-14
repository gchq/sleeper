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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class StateStoreCommitterRun {

    private final String logStream;
    private final Instant startTime;
    private final Instant finishTime;
    private final List<StateStoreCommitSummary> commits;

    public StateStoreCommitterRun(String logStream, Instant startTime, Instant finishTime, List<StateStoreCommitSummary> commits) {
        this.logStream = logStream;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.commits = commits;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    public List<StateStoreCommitSummary> getCommits() {
        return commits;
    }

    public Stream<StateStoreCommitterLogEntry> logs() {
        return Stream.of(
                Optional.ofNullable(startTime).map(time -> new StateStoreCommitterRunStarted(logStream, time)).stream(),
                commits.stream(),
                Optional.ofNullable(finishTime).map(time -> new StateStoreCommitterRunFinished(logStream, time)).stream())
                .flatMap(s -> s);
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
