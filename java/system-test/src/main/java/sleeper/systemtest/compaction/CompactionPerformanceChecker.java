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

public class CompactionPerformanceChecker {
    private CompactionPerformanceChecker() {
    }

    public static void check(CompactionPerformanceResults results,
                             CompactionPerformanceResults expectedResults) throws CheckFailedException {
        check(results, expectedResults.getNumOfJobs(), expectedResults.getNumOfRecordsInRoot(),
                expectedResults.getReadRate(), expectedResults.getWriteRate());
    }

    public static void check(CompactionPerformanceResults results,
                             int expectedNumOfCompactionJobs, int expectedNumOfRecordsinRoot,
                             double previousReadRate, double previousWriteRate) throws CheckFailedException {
        if (results.getNumOfJobs() != expectedNumOfCompactionJobs) {
            throw new CheckFailedException("Actual number of compaction jobs " + results.getNumOfJobs() +
                    " does not match expected number of jobs " + expectedNumOfCompactionJobs);
        }
        if (results.getNumOfRecordsInRoot() != expectedNumOfRecordsinRoot) {
            throw new CheckFailedException("Actual number of records in root partition " + results.getNumOfRecordsInRoot() +
                    " does not match expected number of records in root partition " + expectedNumOfRecordsinRoot);
        }
        if (results.getReadRate() < previousReadRate) {
            throw new CheckFailedException("Read rate " + results.getReadRate() +
                    " is worse than read rate from previous performance test " + previousReadRate);
        }
        if (results.getWriteRate() < previousWriteRate) {
            throw new CheckFailedException("Write rate " + results.getWriteRate() +
                    " is worse than write rate from previous performance test " + previousWriteRate);
        }
    }


    static class CheckFailedException extends IllegalStateException {
        CheckFailedException(String msg) {
            super(msg);
        }
    }
}
