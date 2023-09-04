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

import sleeper.core.record.process.status.ProcessRun;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

public class AverageRecordRate {

    private final int runCount;
    private final long recordsRead;
    private final long recordsWritten;
    private final Duration totalDuration;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;
    private final double averageJobRecordsReadPerSecond;
    private final double averageJobRecordsWrittenPerSecond;

    private AverageRecordRate(Builder builder) {
        runCount = builder.runCount;
        recordsRead = builder.recordsRead;
        recordsWritten = builder.recordsWritten;
        if (builder.startTime == null || builder.finishTime == null) {
            totalDuration = builder.totalRunDuration;
        } else {
            totalDuration = Duration.between(builder.startTime, builder.finishTime);
        }
        double totalSeconds = totalDuration.toMillis() / 1000.0;
        recordsReadPerSecond = recordsRead / totalSeconds;
        recordsWrittenPerSecond = recordsWritten / totalSeconds;
        averageJobRecordsReadPerSecond = builder.totalRecordsReadPerSecond / builder.runsWithRecordsRead;
        averageJobRecordsWrittenPerSecond = builder.totalRecordsWrittenPerSecond / builder.runsWithRecordsWritten;
    }

    public static AverageRecordRate of(Stream<ProcessRun> runs) {
        return builder().summaries(runs
                .filter(ProcessRun::isFinished)
                .map(ProcessRun::getFinishedSummary)).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getRunCount() {
        return runCount;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public Duration getTotalDuration() {
        return totalDuration;
    }

    public double getRecordsReadPerSecond() {
        return recordsReadPerSecond;
    }

    public double getRecordsWrittenPerSecond() {
        return recordsWrittenPerSecond;
    }

    public double getAverageRunRecordsReadPerSecond() {
        return averageJobRecordsReadPerSecond;
    }

    public double getAverageRunRecordsWrittenPerSecond() {
        return averageJobRecordsWrittenPerSecond;
    }

    @Override
    public String toString() {
        return "AverageRecordRate{" +
                "runCount=" + runCount +
                ", recordsRead=" + recordsRead +
                ", recordsWritten=" + recordsWritten +
                ", totalDuration=" + totalDuration +
                ", recordsReadPerSecond=" + recordsReadPerSecond +
                ", recordsWrittenPerSecond=" + recordsWrittenPerSecond +
                ", averageJobRecordsReadPerSecond=" + averageJobRecordsReadPerSecond +
                ", averageJobRecordsWrittenPerSecond=" + averageJobRecordsWrittenPerSecond +
                '}';
    }

    public static final class Builder {
        private Instant startTime;
        private Instant finishTime;
        private int runCount;
        private long recordsRead;
        private long recordsWritten;
        private Duration totalRunDuration = Duration.ZERO;
        private int runsWithRecordsRead;
        private Duration totalReadingDuration = Duration.ZERO;
        private double totalRecordsReadPerSecond;
        private int runsWithRecordsWritten;
        private Duration totalWritingDuration = Duration.ZERO;
        private double totalRecordsWrittenPerSecond;

        private Builder() {
        }

        public Builder summaries(Stream<RecordsProcessedSummary> summaries) {
            summaries.forEach(this::summary);
            return this;
        }

        public Builder summary(RecordsProcessedSummary summary) {
            runCount++;
            recordsRead += summary.getRecordsRead();
            recordsWritten += summary.getRecordsWritten();
            totalRunDuration = totalRunDuration.plus(summary.getTimeInProcess());
            if (!summary.getTimeInProcess().isZero()) { // Can't calculate average rate accurately if duration is zero
                if (summary.getRecordsReadPerSecond() > 0) {
                    runsWithRecordsRead++;
                    totalReadingDuration = totalReadingDuration.plus(summary.getTimeInProcess());
                    totalRecordsReadPerSecond += summary.getRecordsReadPerSecond();
                }
                if (summary.getRecordsWrittenPerSecond() > 0) {
                    runsWithRecordsWritten++;
                    totalWritingDuration = totalWritingDuration.plus(summary.getTimeInProcess());
                    totalRecordsWrittenPerSecond += summary.getRecordsWrittenPerSecond();
                }
            }
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

        public AverageRecordRate build() {
            return new AverageRecordRate(this);
        }
    }
}
