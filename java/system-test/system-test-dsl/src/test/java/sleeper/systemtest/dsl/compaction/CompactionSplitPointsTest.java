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
package sleeper.systemtest.dsl.compaction;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.SystemTestSplitPointsHelper;

@InMemoryDslTest
public class CompactionSplitPointsTest {

    @Test
    @Disabled
    void shouldCreateLargeQuanitiesOfCompactionJobsAtOnce(SleeperSystemTest sleeper) {
        // Given
        //sleeper.generateNumberedRecords(null, null)

        // When
        SystemTestSplitPointsHelper.createPartitionTreeWithRecordsPerPartitionAndTotal(sleeper, 0, 0);

        // Then

    }

}
