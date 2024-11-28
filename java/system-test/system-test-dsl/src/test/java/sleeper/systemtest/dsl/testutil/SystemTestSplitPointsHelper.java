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
package sleeper.systemtest.dsl.testutil;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionTreeTestHelper;
import sleeper.systemtest.dsl.SleeperSystemTest;

public class SystemTestSplitPointsHelper {

    private SystemTestSplitPointsHelper() {
    }

    public static PartitionTree createPartitionTreeWithRecordsPerPartitionAndTotal(SleeperSystemTest sleeper, int recordsPerPartition, int totalRecords) {
        return PartitionTreeTestHelper.createPartitionTreeWithRecordsPerPartitionAndTotal(recordsPerPartition, totalRecords, sleeper.generateNumberedRecords()::numberedRecord,
                sleeper.tableProperties().getSchema());
    }

}