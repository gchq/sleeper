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

import sleeper.core.record.SleeperRow;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.model.IngestQueue.STANDARD_INGEST;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestIngestMode.QUEUE;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class SetupInstanceST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @Test
    void shouldConnectToInstance(SleeperSystemTest sleeper) {
        assertThat(sleeper.instanceProperties().getBoolean(RETAIN_INFRA_AFTER_DESTROY))
                .isFalse();
    }

    @Test
    void shouldIngestOneRecord(SleeperSystemTest sleeper) {
        // Given
        SleeperRow record = new SleeperRow(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));

        // When
        sleeper.ingest().direct(tempDir).records(record);

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
    }

    @Test
    void shouldIngestByQueueWithSystemTestCluster(SleeperSystemTest sleeper) {
        if (sleeper.systemTestCluster().isDisabled()) {
            return;
        }

        // When
        sleeper.systemTestCluster().runDataGenerationJobs(2,
                builder -> builder.ingestMode(QUEUE).ingestQueue(STANDARD_INGEST).recordsPerIngest(123))
                .waitForStandardIngestTask().waitForIngestJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .hasSize(246);
        assertThat(sleeper.systemTestCluster().findIngestJobIdsInSourceBucket())
                .hasSize(2)
                .containsExactlyInAnyOrderElementsOf(sleeper.reporting().ingestJobs().jobIds());
    }

    @Test
    void shouldIngestDirectlyWithSystemTestCluster(SleeperSystemTest sleeper) {
        if (sleeper.systemTestCluster().isDisabled()) {
            return;
        }

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
