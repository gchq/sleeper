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
package sleeper.systemtest.dsl.bulkexport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class BulkExportTest {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) throws Exception {
        sleeper.connectToInstanceNoTables(IN_MEMORY_MAIN);
    }

    @Test
    void shouldPerformBasicBulkExport(SleeperSystemTest sleeper) throws Exception {
        // Given
        //instanceProperties.set(BULK_EXPORT_QUEUE_URL,

        //createSqsQueueGetUrl());
        BulkExportQuery query = BulkExportQuery.builder()
                .exportId("test-export")
                .tableName("table-name")
                .build();

        // When
        sleeper.bulkExport().sendBulkExportQuery(query);

        // Then
        //assertThat(recieveBulkExportQueries()).containsExactly(query);
    }

}
