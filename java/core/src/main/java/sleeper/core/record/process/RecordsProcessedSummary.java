/*
 * Copyright 2022 Crown Copyright
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
package sleeper.core.record.process;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class RecordsProcessedSummary {

    private final RecordsProcessed recordsProcessed;
    private final Instant startTime;
    private final Instant finishTime;
    private final double durationInSeconds;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    public RecordsProcessedSummary(RecordsProcessed recordsProcessed, Instant startTime, Instant finishTime) {
        this.recordsProcessed = Objects.requireNonNull(recordsProcessed, "recordsProcessed must not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime must not be null");
        this.finishTime = Objects.requireNonNull(finishTime, "finishTime must not be null");
        this.durationInSeconds = Duration.between(startTime, finishTime).toMillis() / 1000.0;
        this.recordsReadPerSecond = recordsProcessed.getLinesRead() / this.durationInSeconds;
        this.recordsWrittenPerSecond = recordsProcessed.getLinesWritten() / this.durationInSeconds;
    }

    public long getLinesRead() {
        return recordsProcessed.getLinesRead();
    }

    public long getLinesWritten() {
        return recordsProcessed.getLinesWritten();
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    public double getDurationInSeconds() {
        return durationInSeconds;
    }

    public double getRecordsReadPerSecond() {
        return recordsReadPerSecond;
    }

    public double getRecordsWrittenPerSecond() {
        return recordsWrittenPerSecond;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordsProcessedSummary that = (RecordsProcessedSummary) o;
        return recordsProcessed.equals(that.recordsProcessed) &&
                startTime.equals(that.startTime) && finishTime.equals(that.finishTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsProcessed, startTime, finishTime);
    }

    @Override
    public String toString() {
        return "RecordsProcessedSummary{" +
                "recordsProcessed=" + recordsProcessed +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", durationInSeconds=" + durationInSeconds +
                ", recordsReadPerSecond=" + recordsReadPerSecond +
                ", recordsWrittenPerSecond=" + recordsWrittenPerSecond +
                '}';
    }
}
