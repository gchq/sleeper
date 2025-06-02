/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.systemtest.dsl.sourcedata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.configurationv2.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class SystemTestClusterTest {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldIngestDirectlyWithSystemTestCluster(SleeperSystemTest sleeper) {
        // When
        sleeper.systemTestCluster().runDataGenerationJobs(2,
                builder -> builder.ingestMode(DIRECT).recordsPerIngest(123))
                .waitForTotalFileReferences(2);

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .hasSize(246);
        assertThat(sleeper.systemTestCluster().findIngestJobIdsInSourceBucket())
                .isEmpty();
    }

}
