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

import java.util.stream.Stream;

public class AverageRecordRate {

    private final int jobCount;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    private AverageRecordRate(Builder builder) {
        jobCount = builder.jobCount;
        recordsReadPerSecond = builder.totalRecordsReadPerSecond / builder.jobCount;
        recordsWrittenPerSecond = builder.totalRecordsWrittenPerSecond / builder.jobCount;
    }

    public static AverageRecordRate of(RecordsProcessedSummary... summaries) {
        return of(Stream.of(summaries));
    }

    public static AverageRecordRate of(Stream<RecordsProcessedSummary> summaries) {
        return builder().summaries(summaries).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getJobCount() {
        return jobCount;
    }

    public double getAverageJobRecordsReadPerSecond() {
        return recordsReadPerSecond;
    }

    public double getAverageJobRecordsWrittenPerSecond() {
        return recordsWrittenPerSecond;
    }

    public static final class Builder {
        private int jobCount;
        private double totalRecordsReadPerSecond;
        private double totalRecordsWrittenPerSecond;

        private Builder() {
        }

        public Builder summaries(Stream<RecordsProcessedSummary> summaries) {
            summaries.forEach(this::summary);
            return this;
        }

        public Builder summary(RecordsProcessedSummary summary) {
            jobCount++;
            totalRecordsReadPerSecond += summary.getRecordsReadPerSecond();
            totalRecordsWrittenPerSecond += summary.getRecordsWrittenPerSecond();
            return this;
        }

        public AverageRecordRate build() {
            return new AverageRecordRate(this);
        }
    }
}
