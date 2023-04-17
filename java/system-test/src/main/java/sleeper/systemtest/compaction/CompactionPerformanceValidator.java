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

import com.google.common.math.IntMath;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.systemtest.SystemTestProperties;

import java.math.RoundingMode;

import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_WRITERS;

public class CompactionPerformanceValidator {
    private final int numberOfJobsExpected;
    private final int numberOfRecordsExpected;

    public CompactionPerformanceValidator(int numberOfJobsExpected, int numberOfRecordsExpected) {
        this.numberOfJobsExpected = numberOfJobsExpected;
        this.numberOfRecordsExpected = numberOfRecordsExpected;
    }

    public static CompactionPerformanceValidator from(
            SystemTestProperties instanceProperties, TableProperties tableProperties) {
        int numberOfJobs = calculateNumberOfJobsExpected(instanceProperties, tableProperties);
        int numberOfRecords = calculateNumberOfRecordsExpected(instanceProperties);
        return new CompactionPerformanceValidator(numberOfJobs, numberOfRecords);
    }

    private static int calculateNumberOfJobsExpected(InstanceProperties properties, TableProperties tableProperties) {
        return IntMath.divide(properties.getInt(NUMBER_OF_WRITERS),
                tableProperties.getInt(TableProperty.COMPACTION_FILES_BATCH_SIZE), RoundingMode.CEILING);
    }

    private static int calculateNumberOfRecordsExpected(InstanceProperties properties) {
        return properties.getInt(NUMBER_OF_WRITERS) * properties.getInt(NUMBER_OF_RECORDS_PER_WRITER);
    }

    public void test(CompactionPerformanceResults results) {
        if (results.getNumberOfJobs() != numberOfJobsExpected) {
            throw new IllegalStateException("Actual number of compaction jobs " + results.getNumberOfJobs() +
                    " did not match expected value " + numberOfJobsExpected);
        }
    }

    public int getNumberOfJobsExpected() {
        return numberOfJobsExpected;
    }

    public int getNumberOfRecordsExpected() {
        return numberOfRecordsExpected;
    }
}
