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
package sleeper.systemtest.dsl.compaction;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.util.PollWithRetries;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class WaitForCompactionJobCreationTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    List<TableProperties> tables = new ArrayList<>();
    InMemoryCompactionJobTracker jobTracker = new InMemoryCompactionJobTracker();

    @Test
    void shouldFindJobsCreatedImmediately() {
        // Given
        TableProperties table = createTable();
        Runnable createJobs = () -> {
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-1")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("L").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:01Z"));
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-2")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("R").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:02Z"));
        };

        // When
        List<String> jobIds = createJobsGetIds(2, createJobs);

        // Then
        assertThat(jobIds).containsExactly("test-job-2", "test-job-1");
    }

    @Test
    void shouldFindNotEnoughJobsCreated() {
        // Given
        TableProperties table = createTable();
        Runnable createJobs = () -> {
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("L").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:01Z"));
        };

        // When / Then
        assertThatThrownBy(() -> createJobsGetIds(2, createJobs))
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    @Test
    void shouldFindMoreThanExpectedJobsCreated() {
        // Given
        TableProperties table = createTable();
        Runnable createJobs = () -> {
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-1")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("L").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:01Z"));
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-2")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("R").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:02Z"));
        };

        // When / Then
        assertThatThrownBy(() -> createJobsGetIds(1, createJobs))
                .isInstanceOf(TooManyCompactionsCreatedException.class)
                .hasMessage("Expected 1 new compaction jobs, found 2");
    }

    @Test
    void shouldAllowExtraJobsCreatedByMin() {
        // Given
        TableProperties table = createTable();
        Runnable createJobs = () -> {
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-1")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("L").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:01Z"));
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-2")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("R").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:02Z"));
        };

        // When
        List<String> jobIds = createMinJobsGetIds(1, createJobs);

        // Then
        assertThat(jobIds).containsExactly("test-job-2", "test-job-1");
    }

    @Test
    void shouldAllowExactJobsCreatedByMin() {
        // Given
        TableProperties table = createTable();
        Runnable createJobs = () -> {
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-1")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("L").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:01Z"));
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job-2")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("R").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:02Z"));
        };

        // When
        List<String> jobIds = createMinJobsGetIds(2, createJobs);

        // Then
        assertThat(jobIds).containsExactly("test-job-2", "test-job-1");
    }

    @Test
    void shouldFindNotEnoughJobsCreatedByMin() {
        // Given
        TableProperties table = createTable();
        Runnable createJobs = () -> {
            jobTracker.jobCreated(CompactionJobCreatedEvent.builder()
                    .jobId("test-job")
                    .tableId(table.get(TABLE_ID))
                    .partitionId("L").inputFilesCount(2)
                    .build(), Instant.parse("2026-03-31T14:00:01Z"));
        };

        // When / Then
        assertThatThrownBy(() -> createMinJobsGetIds(2, createJobs))
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    private TableProperties createTable() {
        TableProperties properties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        tables.add(properties);
        return properties;
    }

    private List<String> createJobsGetIds(int expectedJobs, Runnable createJobs) {
        return waitForJobs().createJobsGetIds(expectedJobs, PollWithRetries.immediateRetries(10), createJobs);
    }

    private List<String> createMinJobsGetIds(int expectedJobs, Runnable createJobs) {
        return waitForJobs().createMinJobsGetIds(expectedJobs, PollWithRetries.immediateRetries(10), createJobs);
    }

    private WaitForCompactionJobCreation waitForJobs() {
        return new WaitForCompactionJobCreation(tables::stream, jobTracker);
    }

}
