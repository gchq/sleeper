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
    private final int numOfJobs;
    private final int numOfRecordsInRoot;
    private final double readRate;
    private final double writeRate;

    private CompactionPerformanceResults(Builder builder) {
        numOfJobs = builder.numOfJobs;
        numOfRecordsInRoot = builder.numOfRecordsInRoot;
        readRate = builder.readRate;
        writeRate = builder.writeRate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getNumOfJobs() {
        return numOfJobs;
    }

    public int getNumOfRecordsInRoot() {
        return numOfRecordsInRoot;
    }

    public double getReadRate() {
        return readRate;
    }

    public double getWriteRate() {
        return writeRate;
    }

    public static final class Builder {
        private int numOfJobs;
        private int numOfRecordsInRoot;
        private double readRate;
        private double writeRate;

        public Builder() {
        }

        public Builder numOfJobs(int numOfJobs) {
            this.numOfJobs = numOfJobs;
            return this;
        }

        public Builder numOfRecordsInRoot(int numOfRecordsInRoot) {
            this.numOfRecordsInRoot = numOfRecordsInRoot;
            return this;
        }

        public Builder readRate(double readRate) {
            this.readRate = readRate;
            return this;
        }

        public Builder writeRate(double writeRate) {
            this.writeRate = writeRate;
            return this;
        }

        public CompactionPerformanceResults build() {
            return new CompactionPerformanceResults(this);
        }
    }
}
