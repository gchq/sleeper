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

public class CompactionPerformanceResults {
    private final int actualNumOfJobs;
    private final int actualNumOfRecordsInRoot;
    private final double actualReadRate;
    private final double actualWriteRate;

    private CompactionPerformanceResults(Builder builder) {
        actualNumOfJobs = builder.actualNumOfJobs;
        actualNumOfRecordsInRoot = builder.actualNumOfRecordsInRoot;
        actualReadRate = builder.actualReadRate;
        actualWriteRate = builder.actualWriteRate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getActualNumOfJobs() {
        return actualNumOfJobs;
    }

    public int getActualNumOfRecordsInRoot() {
        return actualNumOfRecordsInRoot;
    }

    public double getActualReadRate() {
        return actualReadRate;
    }

    public double getActualWriteRate() {
        return actualWriteRate;
    }

    public static final class Builder {
        private int actualNumOfJobs;
        private int actualNumOfRecordsInRoot;
        private double actualReadRate;
        private double actualWriteRate;

        public Builder() {
        }

        public Builder actualNumOfJobs(int actualNumOfJobs) {
            this.actualNumOfJobs = actualNumOfJobs;
            return this;
        }

        public Builder actualNumOfRecordsInRoot(int actualNumOfRecordsInRoot) {
            this.actualNumOfRecordsInRoot = actualNumOfRecordsInRoot;
            return this;
        }

        public Builder actualReadRate(double actualReadRate) {
            this.actualReadRate = actualReadRate;
            return this;
        }

        public Builder actualWriteRate(double actualWriteRate) {
            this.actualWriteRate = actualWriteRate;
            return this;
        }

        public CompactionPerformanceResults build() {
            return new CompactionPerformanceResults(this);
        }
    }
}
