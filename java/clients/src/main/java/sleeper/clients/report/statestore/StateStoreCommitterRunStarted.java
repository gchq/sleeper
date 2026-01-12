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

import java.time.Instant;
import java.util.Objects;

/**
 * A log entry recording that a state store committer invocation started.
 */
public class StateStoreCommitterRunStarted implements StateStoreCommitterLogEntry {
    private final String logStream;
    private final Instant timestamp;
    private final Instant startTime;

    public StateStoreCommitterRunStarted(String logStream, Instant timestamp, Instant startTime) {
        this.logStream = logStream;
        this.timestamp = timestamp;
        this.startTime = startTime;
    }

    @Override
    public String getLogStream() {
        return logStream;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public Instant getTimeInCommitter() {
        return startTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(logStream, timestamp, startTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitterRunStarted)) {
            return false;
        }
        StateStoreCommitterRunStarted other = (StateStoreCommitterRunStarted) obj;
        return Objects.equals(logStream, other.logStream) && Objects.equals(timestamp, other.timestamp) && Objects.equals(startTime, other.startTime);
    }

    @Override
    public String toString() {
        return "StateStoreCommitterRunStarted{logStream=" + logStream + ", timestamp=" + timestamp + ", startTime=" + startTime + "}";
    }
}
