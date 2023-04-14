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

public class CompactionPerformanceCheckerInMemory implements CompactionPerformanceChecker {
    private final int actualNumOfJobs;

    private CompactionPerformanceCheckerInMemory(Builder builder) {
        actualNumOfJobs = builder.actualNumOfJobs;
    }


    @Override
    public void check(int expectedNumOfCompactionJobs, int expectedNumOfRecordsinRoot, double previousReadPerformance, double previousWritePerformance) throws CheckFailedException {
        if (actualNumOfJobs != expectedNumOfCompactionJobs) {
            throw new CheckFailedException("Actual number of compaction jobs " + actualNumOfJobs +
                    " does not match expected number of jobs " + expectedNumOfCompactionJobs);
        }
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder {
        private int actualNumOfJobs;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder actualNumOfJobs(int actualNumOfJobs) {
            this.actualNumOfJobs = actualNumOfJobs;
            return this;
        }

        public CompactionPerformanceCheckerInMemory build() {
            return new CompactionPerformanceCheckerInMemory(this);
        }
    }
}
