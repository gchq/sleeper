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
 * A log entry recording that a state store committer invocation terminated.
 */
public class StateStoreCommitterRunFinished implements StateStoreCommitterLogEntry {
    private final String logStream;
    private final Instant timestamp;
    private final Instant finishTime;

    public StateStoreCommitterRunFinished(String logStream, Instant timestamp, Instant finishTime) {
        this.logStream = logStream;
        this.timestamp = timestamp;
        this.finishTime = finishTime;
    }

    @Override
    public String getLogStream() {
        return logStream;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    @Override
    public Instant getTimeInCommitter() {
        return finishTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(logStream, timestamp, finishTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitterRunFinished)) {
            return false;
        }
        StateStoreCommitterRunFinished other = (StateStoreCommitterRunFinished) obj;
        return Objects.equals(logStream, other.logStream) && Objects.equals(timestamp, other.timestamp) && Objects.equals(finishTime, other.finishTime);
    }

    @Override
    public String toString() {
        return "StateStoreCommitterRunFinished{logStream=" + logStream + ", timestamp=" + timestamp + ", finishTime=" + finishTime + "}";
    }
}
