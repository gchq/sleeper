/*
 * Copyright 2022-2023 Crown Copyright
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
    private final Duration duration;
    private final Duration timeInProcess;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    public RecordsProcessedSummary(RecordsProcessed recordsProcessed, Instant startTime, Duration duration) {
        this(recordsProcessed, startTime, startTime.plus(duration), duration);
    }

    public RecordsProcessedSummary(RecordsProcessed recordsProcessed, Instant startTime, Instant finishTime) {
        this(recordsProcessed, startTime, finishTime, Duration.between(startTime, finishTime));
    }

    public RecordsProcessedSummary(RecordsProcessed recordsProcessed, Instant startTime, Instant finishTime, Duration timeInProcess) {
        this.recordsProcessed = Objects.requireNonNull(recordsProcessed, "recordsProcessed must not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime must not be null");
        this.finishTime = Objects.requireNonNull(finishTime, "finishTime must not be null");
        this.timeInProcess = Objects.requireNonNull(timeInProcess, "timeInProcess must not be null");
        this.duration = Duration.between(startTime, finishTime);
        this.recordsReadPerSecond = recordsProcessed.getRecordsRead() / (this.timeInProcess.toMillis() / 1000.0);
        this.recordsWrittenPerSecond = recordsProcessed.getRecordsWritten() / (this.timeInProcess.toMillis() / 1000.0);
    }

    public static RecordsProcessedSummary noProcessingDoneAtTime(Instant startTime) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(0, 0),
                startTime, Duration.ZERO);
    }

    public long getRecordsRead() {
        return recordsProcessed.getRecordsRead();
    }

    public long getRecordsWritten() {
        return recordsProcessed.getRecordsWritten();
    }

    public RecordsProcessed getRecordsProcessed() {
        return recordsProcessed;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    public double getDurationInSeconds() {
        return duration.toMillis() / 1000.0;
    }

    public Duration getDuration() {
        return duration;
    }

    public Duration getTimeInProcess() {
        return timeInProcess;
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
                startTime.equals(that.startTime) && finishTime.equals(that.finishTime) &&
                timeInProcess.equals(that.timeInProcess);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsProcessed, startTime, finishTime, timeInProcess);
    }

    @Override
    public String toString() {
        return "RecordsProcessedSummary{" +
                "recordsProcessed=" + recordsProcessed +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", duration=" + duration +
                ", timeInProcess=" + timeInProcess +
                ", recordsReadPerSecond=" + recordsReadPerSecond +
                ", recordsWrittenPerSecond=" + recordsWrittenPerSecond +
                '}';
    }
}
