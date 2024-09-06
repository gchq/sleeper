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
package sleeper.bulkimport.starter.executor;

import com.google.common.collect.Lists;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.starter.executor.BulkImportExecutor.WriteJobToBucket;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.acceptedAndFailedToStartIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.acceptedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.rejectedRun;

class BulkImportExecutorTest {
    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            .withIgnoredFieldsMatchingRegexes("jobRun.+updateTime").build();

    private static final Schema SCHEMA = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
    private final String bucketName = UUID.randomUUID().toString();
    private final String tableId = tableProperties.get(TABLE_ID);
    private final IngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();
    private final List<BulkImportJob> jobsInBucket = new ArrayList<>();
    private final List<String> jobRunIdsOfJobsInBucket = new ArrayList<>();
    private final List<BulkImportJob> jobsRun = new ArrayList<>();
    private final List<String> jobRunIdsOfJobsRun = new ArrayList<>();

    @BeforeEach
    void setup() {
        tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
    }

    @Nested
    @DisplayName("Failing validation")
    class FailValidation {
        @Test
        void shouldFailValidationIfFileListIsEmpty() {
            // Given
            BulkImportJob importJob = jobForTable()
                    .id("my-job")
                    .files(Lists.newArrayList())
                    .build();
            Instant validationTime = Instant.parse("2023-06-02T15:41:00Z");

            // When
            executor(atTime(validationTime)).runJob(importJob);

            // Then
            assertThat(jobsInBucket).isEmpty();
            assertThat(jobsRun).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                    .containsExactly(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), validationTime,
                                    "The input files must be set to a non-null and non-empty value.")));
        }

        @Test
        void shouldFailValidationIfJobIdContainsMoreThan63Characters() {
            // Given
            String invalidId = UUID.randomUUID().toString() + UUID.randomUUID();
            BulkImportJob importJob = jobForTable()
                    .files(Lists.newArrayList("file1.parquet"))
                    .id(invalidId)
                    .build();
            Instant validationTime = Instant.parse("2023-06-02T15:41:00Z");
            // When
            executor(atTime(validationTime)).runJob(importJob);

            // Then
            assertThat(jobsInBucket).isEmpty();
            assertThat(jobsRun).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                    .containsExactly(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), validationTime,
                                    "Job IDs are only allowed to be up to 63 characters long.")));
        }

        @Test
        void shouldFailValidationIfJobIdContainsUppercaseLetters() {
            // Given
            BulkImportJob importJob = jobForTable()
                    .id("importJob")
                    .files(Lists.newArrayList("file1.parquet"))
                    .build();
            Instant validationTime = Instant.parse("2023-06-02T15:41:00Z");

            // When
            executor(atTime(validationTime)).runJob(importJob);

            // Then
            assertThat(jobsInBucket).isEmpty();
            assertThat(jobsRun).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                    .containsExactly(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), validationTime,
                                    "Job Ids must only contain lowercase alphanumerics and dashes.")));
        }
    }

    @Test
    void shouldCallRunOnPlatformIfJobIsValid() {
        // Given
        BulkImportJob importJob = jobForTable()
                .id("my-job")
                .files(List.of(
                        bucketName + "/file1.parquet",
                        bucketName + "/file2.parquet",
                        bucketName + "/directory/file3.parquet"))
                .build();
        Instant validationTime = Instant.parse("2023-06-02T15:41:00Z");

        // When
        executor(atTime(validationTime)).runJob(importJob, "job-run-id");

        // Then
        assertThat(jobsInBucket).containsExactly(importJob);
        assertThat(jobRunIdsOfJobsInBucket).containsExactly("job-run-id");
        assertThat(jobsRun).containsExactly(importJob);
        assertThat(jobRunIdsOfJobsRun).containsExactly("job-run-id");
        assertThat(ingestJobStatusStore.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(importJob.toIngestJob(),
                        acceptedRun(importJob.toIngestJob(), validationTime)));
    }

    @Test
    void shouldSucceedIfS3ObjectIsADirectoryContainingFiles() {
        // Given
        BulkImportJob importJob = jobForTable()
                .id("my-job")
                .files(List.of(bucketName + "/directory", bucketName + "/directory/"))
                .build();
        Instant validationTime = Instant.parse("2023-06-02T15:41:00Z");

        // When
        executor(atTime(validationTime)).runJob(importJob, "job-run-id");

        // Then
        assertThat(jobsInBucket).containsExactly(importJob);
        assertThat(jobRunIdsOfJobsInBucket).containsExactly("job-run-id");
        assertThat(jobsRun).containsExactly(importJob);
        assertThat(jobRunIdsOfJobsRun).containsExactly("job-run-id");
        assertThat(ingestJobStatusStore.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(importJob.toIngestJob(),
                        acceptedRun(importJob.toIngestJob(), validationTime)));
    }

    @Test
    void shouldDoNothingWhenJobIsNull() {
        // When
        executor(noTimes()).runJob(null);

        // Then
        assertThat(jobsInBucket).isEmpty();
        assertThat(jobsRun).isEmpty();
    }

    @Test
    void shouldFailJobRunWhenWriteToBucketFails() {
        // Given
        BulkImportJob importJob = jobForTable()
                .id("some-job")
                .files(List.of(
                        bucketName + "/file1.parquet",
                        bucketName + "/file2.parquet",
                        bucketName + "/directory/file3.parquet"))
                .build();
        Instant validationTime = Instant.parse("2023-06-02T15:41:00Z");
        Instant failureTime = Instant.parse("2023-06-02T15:41:05Z");
        RuntimeException rootCause = new RuntimeException("Some root cause");
        RuntimeException cause = new RuntimeException("Some cause", rootCause);
        RuntimeException failure = new RuntimeException("Unexpected failure", cause);

        // When / Then
        assertThatThrownBy(() -> executor(
                writeJobToBucketFails(failure), recordPlatformExecutor(), atTimes(validationTime, failureTime))
                .runJob(importJob, "some-job-run"))
                .isSameAs(failure);
        assertThat(jobsRun).isEmpty();
        assertThat(ingestJobStatusStore.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(importJob.toIngestJob(),
                        acceptedAndFailedToStartIngestRun(importJob.toIngestJob(), validationTime, failureTime,
                                List.of("Unexpected failure", "Some cause", "Some root cause"))));
    }

    @Test
    void shouldFailJobRunWhenPlatformExecutorFails() {
        // Given
        BulkImportJob importJob = jobForTable()
                .id("some-job")
                .files(List.of(
                        bucketName + "/file1.parquet",
                        bucketName + "/file2.parquet",
                        bucketName + "/directory/file3.parquet"))
                .build();
        Instant validationTime = Instant.parse("2023-06-02T15:41:00Z");
        Instant failureTime = Instant.parse("2023-06-02T15:41:05Z");
        RuntimeException rootCause = new RuntimeException("Some root cause");
        RuntimeException cause = new RuntimeException("Some cause", rootCause);
        RuntimeException failure = new RuntimeException("Unexpected failure", cause);

        // When / Then
        assertThatThrownBy(() -> executor(
                recordWriteJobToBucket(), platformExecutorFails(failure), atTimes(validationTime, failureTime))
                .runJob(importJob, "some-job-run"))
                .isSameAs(failure);
        assertThat(jobsInBucket).contains(importJob);
        assertThat(ingestJobStatusStore.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(importJob.toIngestJob(),
                        acceptedAndFailedToStartIngestRun(importJob.toIngestJob(), validationTime, failureTime,
                                List.of("Unexpected failure", "Some cause", "Some root cause"))));
    }

    private BulkImportJob.Builder jobForTable() {
        return BulkImportJob.builder().tableId(tableId).tableName(tableProperties.get(TABLE_NAME));
    }

    public BulkImportExecutor executor(Supplier<Instant> timeSupplier) {
        return executor(recordWriteJobToBucket(), recordPlatformExecutor(), timeSupplier);
    }

    private BulkImportExecutor executor(
            WriteJobToBucket writeJobToBucket, PlatformExecutor platformExecutor, Supplier<Instant> timeSupplier) {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tableProperties,
                inMemoryStateStoreWithFixedSinglePartition(SCHEMA));
        return new BulkImportExecutor(instanceProperties, tablePropertiesProvider, stateStoreProvider,
                ingestJobStatusStore, writeJobToBucket, platformExecutor, timeSupplier);
    }

    private WriteJobToBucket writeJobToBucketFails(RuntimeException failure) {
        return (job, jobRunId) -> {
            throw failure;
        };
    }

    private PlatformExecutor platformExecutorFails(RuntimeException failure) {
        return (arguments) -> {
            throw failure;
        };
    }

    private WriteJobToBucket recordWriteJobToBucket() {
        return new RecordWriteJobToBucket();
    }

    private PlatformExecutor recordPlatformExecutor() {
        return new RecordPlatformExecutor();
    }

    private Supplier<Instant> atTime(Instant validationTime) {
        return List.of(validationTime).iterator()::next;
    }

    private Supplier<Instant> atTimes(Instant... times) {
        return List.of(times).iterator()::next;
    }

    private Supplier<Instant> noTimes() {
        return List.<Instant>of().iterator()::next;
    }

    private class RecordPlatformExecutor implements PlatformExecutor {
        @Override
        public void runJobOnPlatform(BulkImportArguments arguments) {
            jobsRun.add(arguments.getBulkImportJob());
            jobRunIdsOfJobsRun.add(arguments.getJobRunId());
        }
    }

    private class RecordWriteJobToBucket implements WriteJobToBucket {
        @Override
        public void writeJobToBulkImportBucket(BulkImportJob bulkImportJob, String jobRunID) {
            jobsInBucket.add(bulkImportJob);
            jobRunIdsOfJobsInBucket.add(jobRunID);
        }
    }

}
