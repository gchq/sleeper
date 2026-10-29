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

import sleeper.core.row.Row;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_LOGS_AFTER_DESTROY;
import static sleeper.core.properties.model.IngestQueue.STANDARD_INGEST;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestIngestMode.QUEUE;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class SetupInstanceST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @Test
    void shouldConnectToInstance(SleeperDsl sleeper) {

        // Then the instance is configured with the expected retention settings
        assertThat(sleeper.instanceProperties().getBoolean(RETAIN_INFRA_AFTER_DESTROY))
                .isFalse();
        assertThat(sleeper.instanceProperties().getBoolean(RETAIN_LOGS_AFTER_DESTROY))
                .isTrue();
    }

    @Test
    void shouldIngestOneRow(SleeperDsl sleeper) {

        // Given a row ready for ingestion
        Row row = new Row();
        row.put("key", "some-id");
        row.put("timestamp", 1234L);
        row.put("value", "Some value");
        row.put("nullable_value", null);

        // When the row is ingested
        sleeper.ingest().direct(tempDir).rows(row);

        // Then the row can be queried from the table
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactly(row);
    }

    @Test
    void shouldIngestByQueueWithSystemTestCluster(SleeperDsl sleeper) {
        if (sleeper.systemTestCluster().isDisabled()) {
            return;
        }

        // When data generation jobs are run using queued ingest
        sleeper.systemTestCluster().runDataGenerationJobs(2,
                builder -> builder.ingestMode(QUEUE).ingestQueue(STANDARD_INGEST).rowsPerIngest(123))
                .waitForStandardIngestTask().waitForIngestJobs();

        // Then all generated rows are ingested successfully
        assertThat(sleeper.directQuery().allRowsInTable())
                .hasSize(246);

        // And the ingest jobs are reported correctly
        assertThat(sleeper.systemTestCluster().findIngestJobIdsInSourceBucket())
                .hasSize(2)
                .containsExactlyInAnyOrderElementsOf(sleeper.reporting().ingestJobs().jobIds());
    }

    @Test
    void shouldIngestDirectlyWithSystemTestCluster(SleeperDsl sleeper) {
        if (sleeper.systemTestCluster().isDisabled()) {
            return;
        }

        // When data generation jobs are run using direct ingest
        sleeper.systemTestCluster().runDataGenerationJobs(2,
                builder -> builder.ingestMode(DIRECT).rowsPerIngest(123))
                .waitForTotalFileReferences(2);

        // Then all generated rows are ingested successfully
        assertThat(sleeper.directQuery().allRowsInTable())
                .hasSize(246);

        // And no ingest job IDs are written to the source bucket
        assertThat(sleeper.systemTestCluster().findIngestJobIdsInSourceBucket())
                .isEmpty();
    }
}
