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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Parsed logs from a run/invocation of the state store committer.
 */
public class StateStoreCommitterRun {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitterRun.class);
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

    /**
     * Computes the number of transactions committed per second in this invocation. Divides the number of transactions
     * committed by the time spent in the invocation.
     *
     * @return the number of commits per second
     */
    public double computeRequestsPerSecond() {
        Instant startTime = getStartTime();
        LOGGER.info("Test 7 - StartTime {}", startTime);
        if (startTime == null) {
            return 0.0;
        }
        Duration duration = Duration.between(startTime, getFinishTimeOrLastCommitTime().orElse(startTime));
        LOGGER.info("Test 8 - duration {}", duration);
        double seconds = duration.toMillis() / 1000.0;
        LOGGER.info("Test 9 - seconds {}", seconds);
        if (seconds <= 0.0) {
            return 0.0;
        }
        return commits.size() / seconds;
    }

    private Optional<Instant> getFinishTimeOrLastCommitTime() {
        Instant finishTime = getFinishTime();
        LOGGER.info("Test 7.5 - Finishtime {}", finishTime);
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

    private Instant getStartTime() {
        if (start == null) {
            return null;
        }
        return start.getTimeInCommitter();
    }

    private Instant getFinishTime() {
        if (finish == null) {
            return null;
        }
        return finish.getTimeInCommitter();
    }

    public List<StateStoreCommitSummary> getCommits() {
        return commits;
    }

    /**
     * Streams through the log entries in this invocation of the state store committer.
     *
     * @return a stream of all log entries
     */
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

    /**
     * A builder for a run/invocation of the state store committer.
     */
    public static class Builder {
        private String logStream;
        private StateStoreCommitterRunStarted start;
        private StateStoreCommitterRunFinished finish;
        private List<StateStoreCommitSummary> commits = new ArrayList<>();

        private Builder() {
        }

        /**
         * Sets the name of the log stream that was produced by this invocation.
         *
         * @param  logStream the log stream name
         * @return           this builder
         */
        public Builder logStream(String logStream) {
            this.logStream = logStream;
            return this;
        }

        /**
         * Sets the log entry at the start of the state store committer invocation.
         *
         * @param  start the log entry recording that the state store committer started
         * @return       this builder
         */
        public Builder start(StateStoreCommitterRunStarted start) {
            this.start = start;
            return this;
        }

        /**
         * Sets the log entry at the end of the state store committer invocation.
         *
         * @param  finish the log entry recording that the state store committer finished
         * @return        this builder
         */
        public Builder finish(StateStoreCommitterRunFinished finish) {
            this.finish = finish;
            return this;
        }

        /**
         * Sets the log entries for transaction commits during the state store committer invocation.
         *
         * @param  commits the log entries for transactions committed
         * @return         this builder
         */
        public Builder commits(List<StateStoreCommitSummary> commits) {
            this.commits = commits;
            return this;
        }

        /**
         * Adds a log entry for a transaction commit during the state store committer invocation.
         *
         * @param  commit the log entry
         * @return        this builder
         */
        public Builder commit(StateStoreCommitSummary commit) {
            commits.add(commit);
            return this;
        }

        public StateStoreCommitterRun build() {
            return new StateStoreCommitterRun(this);
        }
    }

}
