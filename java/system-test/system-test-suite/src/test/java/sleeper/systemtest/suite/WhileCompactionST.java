/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.compaction.CompactionDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.query.QueryDsl.BackgroundQueries;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class WhileCompactionST {

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(MAIN);
        sleeper.tables().createWithProperties("compaction", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "1"));
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @Test
    void shouldAllowQueriesWhileCompactionsAreRunning(SleeperDsl sleeper) throws Exception {
        // Given many files requiring compaction
        sleeper.updateTableProperties(Map.of(
                INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF.toString()));

        for (int i = 0; i < 200; i++) {
            sleeper.ingest().direct(tempDir)
                    .numberedRows(LongStream.range(0, 1_000));
        }

        // When compactions are running and queries are executed concurrently
        CompactionDsl compaction = sleeper.compaction();
        // compaction.createJobs(20);
        compaction.waitForTasks(1);

        BackgroundQueries queries = sleeper.directQuery()
                .startContinuousQueries();

        compaction.waitForJobs();
        queries.stopAndWait();

        // Then queries succeed
        assertThat(queries.getFailures()).isEmpty();
        assertThat(queries.getQueriesCompleted()).isGreaterThan(0);

        // And all data is preserved after compaction
        assertThat(sleeper.directQuery().allRowsInTable())
                .hasSize(200_000);

    }
}
