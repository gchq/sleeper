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
package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class DataFileDuplicationDslTest {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldDuplicateFiles(SleeperSystemTest sleeper) {
        // Given
        sleeper.ingest().direct(null)
                .numberedRows(LongStream.of(1, 2, 3));

        // When
        sleeper.ingest().toStateStore()
                .duplicateFilesOnSamePartition(2, sleeper.tableFiles().references());

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(3);
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows(LongStream.of(1, 1, 1, 2, 2, 2, 3, 3, 3)));
    }

    @Test
    void shouldCreateCompactionsFromDuplicates(SleeperSystemTest sleeper) {
        // Given
        sleeper.ingest().direct(null)
                .numberedRows(LongStream.of(1, 2, 3));
        DataFileDuplications duplications = sleeper.ingest().toStateStore()
                .duplicateFilesOnSamePartition(2, sleeper.tableFiles().references());

        // When
        sleeper.compaction()
                .createSeparateCompactionsForOriginalAndDuplicates(duplications);
        // .waitForTasks(1).waitForJobs();

        // Then TODO
    }

}
