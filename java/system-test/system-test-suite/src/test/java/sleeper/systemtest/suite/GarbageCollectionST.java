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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class GarbageCollectionST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldGarbageCollectFilesAfterCompaction(SleeperSystemTest sleeper) {
        // Given
        int numberOfFilesToGC = 11_000;
        int rowsPerFile = 100;
        int numberOfRows = rowsPerFile * numberOfFilesToGC;
        sleeper.tables().createWithProperties("gc", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        sleeper.setGeneratorOverrides(overrideField(ROW_KEY_FIELD_NAME,
                numberStringAndZeroPadTo(5).then(addPrefix("row-"))));
        RowNumbers rows = sleeper.scrambleNumberedRows(LongStream.range(0, numberOfRows));
        sleeper.ingest().direct(tempDir)
                .splitIngests(numberOfFilesToGC, rows);
        sleeper.stateStore().fakeCommits().compactAllFilesToOnePerPartition();

        // When
        sleeper.garbageCollection().invoke().waitFor(
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(1)));

        // Then
        assertThat(sleeper.tableFiles().all()).satisfies(files -> {
            assertThat(files.getFilesWithNoReferences()).isEmpty();
            assertThat(files.streamFileReferences()).hasSize(1);
        });
    }
}
