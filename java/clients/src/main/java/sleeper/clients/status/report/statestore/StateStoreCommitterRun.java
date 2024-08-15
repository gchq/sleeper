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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class StateStoreCommitterRun {

    private final String logStream;
    private final StateStoreCommitterRunStarted start;
    private final StateStoreCommitterRunFinished finish;
    private final List<StateStoreCommitSummary> commits;

    public StateStoreCommitterRun(Builder builder) {
        logStream = Objects.requireNonNull(builder.logStream, "logStream must not be null");
        start = builder.start;
        finish = builder.finish;
        commits = Objects.requireNonNull(builder.commits, "commits must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<StateStoreCommitterRun> splitIntoRuns(List<StateStoreCommitterLogEntry> logs) {
        List<StateStoreCommitterRun> runs = new ArrayList<>();
        Map<String, Builder> lastRunByLogStream = new LinkedHashMap<>();
        for (StateStoreCommitterLogEntry entry : logs) {
            if (entry instanceof StateStoreCommitterRunStarted) {
                Optional.ofNullable(lastRunByLogStream.remove(entry.getLogStream()))
                        .ifPresent(builder -> runs.add(builder.build()));
                lastRunByLogStream.put(entry.getLogStream(),
                        newRun(entry).start((StateStoreCommitterRunStarted) entry));
            } else if (entry instanceof StateStoreCommitSummary) {
                lastRunOrNew(lastRunByLogStream, entry)
                        .commit((StateStoreCommitSummary) entry);
            } else if (entry instanceof StateStoreCommitterRunFinished) {
                runs.add(Optional.ofNullable(lastRunByLogStream.remove(entry.getLogStream()))
                        .orElseGet(() -> newRun(entry))
                        .finish((StateStoreCommitterRunFinished) entry)
                        .build());
            }
        }
        lastRunByLogStream.values().forEach(builder -> runs.add(builder.build()));
        return runs;
    }

    private static Builder lastRunOrNew(Map<String, Builder> lastRunByLogStream, StateStoreCommitterLogEntry entry) {
        return lastRunByLogStream.computeIfAbsent(entry.getLogStream(), stream -> newRun(entry));
    }

    private static Builder newRun(StateStoreCommitterLogEntry entry) {
        return builder().logStream(entry.getLogStream()).commits(new ArrayList<>());
    }

    public double computeRequestsPerSecond() {
        Instant startTime = getStartTime();
        if (startTime == null) {
            return 0.0;
        }
        Duration duration = Duration.between(startTime, getFinishTimeOrLastCommitTime().orElse(startTime));
        double seconds = duration.toMillis() / 1000.0;
        if (seconds <= 0.0) {
            return 0.0;
        }
        return commits.size() / seconds;
    }

    private Optional<Instant> getFinishTimeOrLastCommitTime() {
        Instant finishTime = getFinishTime();
        if (finishTime != null) {
            return Optional.of(finishTime);
        }
        return commits.stream()
                .map(StateStoreCommitSummary::getFinishTime)
                .max(Comparator.naturalOrder());
    }

    public String getLogStream() {
        return logStream;
    }

    public Instant getStartTime() {
        if (start == null) {
            return null;
        }
        return start.getTimeInCommitter();
    }

    public Instant getFinishTime() {
        if (finish == null) {
            return null;
        }
        return finish.getTimeInCommitter();
    }

    public List<StateStoreCommitSummary> getCommits() {
        return commits;
    }

    public Stream<StateStoreCommitterLogEntry> entries() {
        return Stream.of(
                Optional.ofNullable(start).stream(),
                commits.stream(),
                Optional.ofNullable(finish).stream())
                .flatMap(s -> s);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logStream, getStartTime(), getFinishTime(), commits);
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
        return Objects.equals(logStream, other.logStream) && Objects.equals(start, other.start) && Objects.equals(finish, other.finish) && Objects.equals(commits, other.commits);
    }

    @Override
    public String toString() {
        return "StateStoreCommitterRun{logStream=" + logStream + ", start=" + start + ", finish=" + finish + ", commits=" + commits + "}";
    }

    public static class Builder {
        private String logStream;
        private StateStoreCommitterRunStarted start;
        private StateStoreCommitterRunFinished finish;
        private List<StateStoreCommitSummary> commits;

        private Builder() {
        }

        public Builder logStream(String logStream) {
            this.logStream = logStream;
            return this;
        }

        public Builder start(StateStoreCommitterRunStarted start) {
            this.start = start;
            return this;
        }

        public Builder finish(StateStoreCommitterRunFinished finish) {
            this.finish = finish;
            return this;
        }

        public Builder commits(List<StateStoreCommitSummary> commits) {
            this.commits = commits;
            return this;
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
