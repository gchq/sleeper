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

import java.util.Objects;

public class CompactionPerformanceResults {
    private final int numOfJobs;
    private final long numOfRecordsInRoot;
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

    public long getNumOfRecordsInRoot() {
        return numOfRecordsInRoot;
    }

    public double getReadRate() {
        return readRate;
    }

    public double getWriteRate() {
        return writeRate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionPerformanceResults results = (CompactionPerformanceResults) o;
        return numOfJobs == results.numOfJobs && numOfRecordsInRoot == results.numOfRecordsInRoot && Double.compare(results.readRate, readRate) == 0 && Double.compare(results.writeRate, writeRate) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numOfJobs, numOfRecordsInRoot, readRate, writeRate);
    }

    @Override
    public String toString() {
        return "CompactionPerformanceResults{" +
                "numOfJobs=" + numOfJobs +
                ", numOfRecordsInRoot=" + numOfRecordsInRoot +
                ", readRate=" + readRate +
                ", writeRate=" + writeRate +
                '}';
    }

    public static final class Builder {
        private int numOfJobs;
        private long numOfRecordsInRoot;
        private double readRate;
        private double writeRate;

        public Builder() {
        }

        public Builder numOfJobs(int numOfJobs) {
            this.numOfJobs = numOfJobs;
            return this;
        }

        public Builder numOfRecordsInRoot(long numOfRecordsInRoot) {
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
