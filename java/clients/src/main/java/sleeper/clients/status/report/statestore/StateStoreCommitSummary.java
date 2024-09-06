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

import java.time.Instant;
import java.util.Objects;

public class StateStoreCommitSummary implements StateStoreCommitterLogEntry {
    private final String logStream;
    private final Instant timestamp;
    private final String tableId;
    private final String type;
    private final Instant finishTime;

    public StateStoreCommitSummary(String logStream, Instant timestamp, String tableId, String type, Instant finishTime) {
        this.logStream = Objects.requireNonNull(logStream, "logStream must not be null");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp must not be null");
        this.tableId = Objects.requireNonNull(tableId, "tableId must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.finishTime = Objects.requireNonNull(finishTime, "finishTime must not be null");
    }

    @Override
    public String getLogStream() {
        return logStream;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    public String getTableId() {
        return tableId;
    }

    public String getType() {
        return type;
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
        return Objects.hash(logStream, timestamp, tableId, type, finishTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitSummary)) {
            return false;
        }
        StateStoreCommitSummary other = (StateStoreCommitSummary) obj;
        return Objects.equals(logStream, other.logStream) && Objects.equals(timestamp, other.timestamp) && Objects.equals(tableId, other.tableId) && Objects.equals(type, other.type)
                && Objects.equals(finishTime, other.finishTime);
    }

    @Override
    public String toString() {
        return "StateStoreCommitSummary{logStream=" + logStream + ", timestamp=" + timestamp + ", tableId=" + tableId + ", type=" + type + ", finishTime=" + finishTime + "}";
    }
}
