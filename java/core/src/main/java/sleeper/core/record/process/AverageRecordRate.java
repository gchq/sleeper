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
package sleeper.core.record.process;

import sleeper.core.record.process.status.ProcessRun;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

/**
 * Calculates records read and written per second across multiple job runs.
 */
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

    /**
     * Creates an instance of this class from a stream of job runs.
     *
     * @param  runs the stream of {@link ProcessRun}s
     * @return      an instance of this class
     */
    public static AverageRecordRate of(Stream<ProcessRun> runs) {
        return builder().summaries(runs
                .filter(ProcessRun::isFinishedSuccessfully)
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

    /**
     * Builder to create a average record rate object.
     */
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

        /**
         * Calculates the average record rate from a stream of records processed summaries.
         *
         * @param  summaries the stream of {@link RecordsProcessedSummary}s
         * @return           the builder
         */
        public Builder summaries(Stream<RecordsProcessedSummary> summaries) {
            summaries.forEach(this::summary);
            return this;
        }

        /**
         * Calculates and updates the average record rate from a records processed summary.
         *
         * @param  summary a {@link RecordsProcessedSummary}
         * @return         the builder
         */
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

        /**
         * Sets the start time.
         *
         * @param  startTime the start time
         * @return           the builder
         */
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the finish time.
         *
         * @param  finishTime the finish time
         * @return            the builder
         */
        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        public AverageRecordRate build() {
            return new AverageRecordRate(this);
        }
    }
}
