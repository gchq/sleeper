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
package sleeper.core.tracker.job.run;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Calculates rows read and written per second across multiple job runs.
 */
public class AverageRowRate {

    private final int runCount;
    private final long rowsRead;
    private final long rowsWritten;
    private final Duration totalDuration;
    private final double rowsReadPerSecond;
    private final double rowsWrittenPerSecond;
    private final double averageRunRowsReadPerSecond;
    private final double averageRunRowsWrittenPerSecond;

    private AverageRowRate(Builder builder) {
        runCount = builder.runCount;
        rowsRead = builder.rowsRead;
        rowsWritten = builder.rowsWritten;
        if (builder.startTime == null || builder.finishTime == null) {
            totalDuration = builder.totalRunDuration;
        } else {
            totalDuration = Duration.between(builder.startTime, builder.finishTime);
        }
        double totalSeconds = totalDuration.toMillis() / 1000.0;
        rowsReadPerSecond = rowsRead / totalSeconds;
        rowsWrittenPerSecond = rowsWritten / totalSeconds;
        averageRunRowsReadPerSecond = builder.totalRowsReadPerSecond / builder.runsWithRowsRead;
        averageRunRowsWrittenPerSecond = builder.totalRowsWrittenPerSecond / builder.runsWithRowsWritten;
    }

    /**
     * Creates an instance of this class from a stream of job runs.
     *
     * @param  runs the stream of {@link JobRun}s
     * @return      an instance of this class
     */
    public static AverageRowRate of(Stream<JobRunReport> runs) {
        return builder().summaries(runs
                .filter(JobRunReport::isFinishedSuccessfully)
                .map(JobRunReport::getFinishedSummary)
                .filter(Objects::nonNull)).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getRunCount() {
        return runCount;
    }

    public long getRowsRead() {
        return rowsRead;
    }

    public long getRowsWritten() {
        return rowsWritten;
    }

    public Duration getTotalDuration() {
        return totalDuration;
    }

    public double getRowsReadPerSecond() {
        return rowsReadPerSecond;
    }

    public double getRowsWrittenPerSecond() {
        return rowsWrittenPerSecond;
    }

    public double getAverageRunRowsReadPerSecond() {
        return averageRunRowsReadPerSecond;
    }

    public double getAverageRunRowsWrittenPerSecond() {
        return averageRunRowsWrittenPerSecond;
    }

    @Override
    public String toString() {
        return "AverageRowRate{" +
                "runCount=" + runCount +
                ", rowsRead=" + rowsRead +
                ", rowsWritten=" + rowsWritten +
                ", totalDuration=" + totalDuration +
                ", rowsReadPerSecond=" + rowsReadPerSecond +
                ", rowsWrittenPerSecond=" + rowsWrittenPerSecond +
                ", averageRunRowsReadPerSecond=" + averageRunRowsReadPerSecond +
                ", averageRunRowsWrittenPerSecond=" + averageRunRowsWrittenPerSecond +
                '}';
    }

    /**
     * Builder to create an average row rate object.
     */
    public static final class Builder {
        private Instant startTime;
        private Instant finishTime;
        private int runCount;
        private long rowsRead;
        private long rowsWritten;
        private Duration totalRunDuration = Duration.ZERO;
        private int runsWithRowsRead;
        private Duration totalReadingDuration = Duration.ZERO;
        private double totalRowsReadPerSecond;
        private int runsWithRowsWritten;
        private Duration totalWritingDuration = Duration.ZERO;
        private double totalRowsWrittenPerSecond;

        private Builder() {
        }

        /**
         * Calculates the average row rate from a stream of rows processed summaries.
         *
         * @param  summaries the stream of {@link JobRunSummary}s
         * @return           the builder
         */
        public Builder summaries(Stream<JobRunSummary> summaries) {
            summaries.forEach(this::summary);
            return this;
        }

        /**
         * Calculates and updates the average row rate from a rows processed summary.
         *
         * @param  summary a {@link JobRunSummary}
         * @return         the builder
         */
        public Builder summary(JobRunSummary summary) {
            runCount++;
            rowsRead += summary.getRowsRead();
            rowsWritten += summary.getRowsWritten();
            totalRunDuration = totalRunDuration.plus(summary.getTimeInProcess());
            if (!summary.getTimeInProcess().isZero()) { // Can't calculate average rate accurately if duration is zero
                if (summary.getRowsReadPerSecond() > 0) {
                    runsWithRowsRead++;
                    totalReadingDuration = totalReadingDuration.plus(summary.getTimeInProcess());
                    totalRowsReadPerSecond += summary.getRowsReadPerSecond();
                }
                if (summary.getRowsWrittenPerSecond() > 0) {
                    runsWithRowsWritten++;
                    totalWritingDuration = totalWritingDuration.plus(summary.getTimeInProcess());
                    totalRowsWrittenPerSecond += summary.getRowsWrittenPerSecond();
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

        public AverageRowRate build() {
            return new AverageRowRate(this);
        }
    }
}
