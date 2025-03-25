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
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_INGESTS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMPACTION_PERFORMANCE;

@SystemTest
@Expensive // Works with a larger set of data requiring the system test cluster
public class GarbageCollectionScaleST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(COMPACTION_PERFORMANCE);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @Test
    void shouldGarbageCollectFilesAfterCompaction(SleeperSystemTest sleeper) {
        // Given
        int numberOfFilesToGC = 40_000;
        int numberOfWriters = 20;
        int ingestsPerWriter = 2000;
        sleeper.tables().createWithProperties("gc", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        sleeper.systemTestCluster().updateProperties(properties -> {
            properties.setEnum(INGEST_MODE, DIRECT);
            properties.setNumber(NUMBER_OF_WRITERS, numberOfWriters);
            properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 100);
            properties.setNumber(NUMBER_OF_INGESTS_PER_WRITER, ingestsPerWriter);
        }).runDataGenerationTasks().waitForTotalFileReferences(numberOfFilesToGC);
        sleeper.stateStore().fakeCommits().compactAllFilesToOnePerPartition();

        // When
        sleeper.garbageCollection().invoke().waitFor(
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(1)));

        // Then
        assertThat(sleeper.tableFiles().all()).satisfies(files -> {
            assertThat(files.getFilesWithNoReferences()).isEmpty();
            assertThat(files.listFileReferences()).hasSize(1);
        });
    }
}
