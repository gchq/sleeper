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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class EmrServerlessBulkImportST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
        purgeQueues.purgeIfTestFailed(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL);
    }

    @Test
    void shouldBulkImportOneRecordWithEmrServerlessByQueue(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1"));
        Record record = new Record(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));

        // When
        sleeper.sourceFiles().create("file.parquet", record);
        sleeper.ingest().bulkImportByQueue().sendSourceFiles(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, "file.parquet")
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
    }

    @Test
    void shouldBulkImportOneRecordWithEmrServerlessDirectly(SleeperSystemTest sleeper) {
        // Given
        Record record = new Record(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));

        // When
        sleeper.sourceFiles().create("file.parquet", record);
        sleeper.ingest().directEmrServerless().sendSourceFiles("file.parquet")
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
    }
}
