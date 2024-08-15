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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public StateStoreCommitterRun(Builder builder) {
        logStream = Objects.requireNonNull(builder.logStream, "logStream must not be null");
        startTime = builder.startTime;
        finishTime = builder.finishTime;
        commits = Objects.requireNonNull(builder.commits, "commits must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<StateStoreCommitterRun> splitIntoRuns(List<StateStoreCommitterLogEntry> logs) {
        List<StateStoreCommitterRun> runs = new ArrayList<>();
        Map<String, Builder> lastRunByLogStream = new LinkedHashMap<>();
        for (StateStoreCommitterLogEntry entry : logs) {
            Builder builder = lastRunByLogStream.computeIfAbsent(entry.getLogStream(),
                    stream -> builder().logStream(stream).commits(new ArrayList<>()));
            if (entry instanceof StateStoreCommitterRunStarted) {
                builder.started((StateStoreCommitterRunStarted) entry);
            } else if (entry instanceof StateStoreCommitSummary) {
                builder.commit((StateStoreCommitSummary) entry);
            } else if (entry instanceof StateStoreCommitterRunFinished) {
                runs.add(builder.finished((StateStoreCommitterRunFinished) entry).build());
                lastRunByLogStream.remove(entry.getLogStream());
            }
        }
        lastRunByLogStream.values().forEach(builder -> runs.add(builder.build()));
        return runs;
    }

    public double computeRequestsPerSecond() {
        return (double) commits.size() /
                (Duration.between(startTime, finishTime).toMillis() / 1000.0);
    }

    public String getLogStream() {
        return logStream;
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

    @Override
    public int hashCode() {
        return Objects.hash(logStream, startTime, finishTime, commits);
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
        return Objects.equals(logStream, other.logStream) && Objects.equals(startTime, other.startTime) && Objects.equals(finishTime, other.finishTime) && Objects.equals(commits, other.commits);
    }

    @Override
    public String toString() {
        return "StateStoreCommitterRun{logStream=" + logStream + ", startTime=" + startTime + ", finishTime=" + finishTime + ", commits=" + commits + "}";
    }

    public static class Builder {
        private String logStream;
        private Instant startTime;
        private Instant finishTime;
        private List<StateStoreCommitSummary> commits;

        private Builder() {
        }

        public Builder logStream(String logStream) {
            this.logStream = logStream;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        public Builder commits(List<StateStoreCommitSummary> commits) {
            this.commits = commits;
            return this;
        }

        private Builder started(StateStoreCommitterRunStarted started) {
            return logStream(started.getLogStream()).startTime(started.getStartTime()).commits(new ArrayList<>());
        }

        private Builder finished(StateStoreCommitterRunFinished finished) {
            return finishTime(finished.getFinishTime());
        }

        private Builder commit(StateStoreCommitSummary commit) {
            commits.add(commit);
            return this;
        }

        public StateStoreCommitterRun build() {
            return new StateStoreCommitterRun(this);
        }
    }

}
