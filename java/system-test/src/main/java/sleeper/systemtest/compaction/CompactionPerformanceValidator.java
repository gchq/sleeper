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
package sleeper.systemtest.compaction;

public class CompactionPerformanceValidator {
    private final int numberOfJobsExpected;
    private final int numberOfRecordsExpected;
    private final double minRecordsPerSecond;

    private CompactionPerformanceValidator(Builder builder) {
        numberOfJobsExpected = builder.numberOfJobsExpected;
        numberOfRecordsExpected = builder.numberOfRecordsExpected;
        minRecordsPerSecond = builder.minRecordsPerSecond;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void test(CompactionPerformanceResults results) {
        if (results.getNumberOfJobs() != numberOfJobsExpected) {
            throw new IllegalStateException("Actual number of compaction jobs " + results.getNumberOfJobs() +
                    " did not match expected value " + numberOfJobsExpected);
        }
        if (results.getNumberOfRecords() != numberOfRecordsExpected) {
            throw new IllegalStateException("Actual number of records " + results.getNumberOfRecords() +
                    " did not match expected value " + numberOfRecordsExpected);
        }
        if (results.getWriteRate() < minRecordsPerSecond) {
            throw new IllegalStateException("Records per second rate of " + results.getWriteRate() + " was slower than expected " + minRecordsPerSecond);
        }
    }

    public static final class Builder {
        private int numberOfJobsExpected;
        private int numberOfRecordsExpected;
        private double minRecordsPerSecond;

        private Builder() {
        }

        public Builder numberOfJobsExpected(int numberOfJobsExpected) {
            this.numberOfJobsExpected = numberOfJobsExpected;
            return this;
        }

        public Builder numberOfRecordsExpected(int numberOfRecordsExpected) {
            this.numberOfRecordsExpected = numberOfRecordsExpected;
            return this;
        }

        public Builder minRecordsPerSecond(double minRecordsPerSecond) {
            this.minRecordsPerSecond = minRecordsPerSecond;
            return this;
        }

        public CompactionPerformanceValidator build() {
            return new CompactionPerformanceValidator(this);
        }
    }
}
