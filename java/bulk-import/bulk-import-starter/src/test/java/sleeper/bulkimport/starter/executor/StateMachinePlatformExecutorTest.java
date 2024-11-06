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

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.rejectedRun;

class StateMachinePlatformExecutorTest {
    private final AWSStepFunctions stepFunctions = mock(AWSStepFunctions.class);
    private final AtomicReference<StartExecutionRequest> requested = new AtomicReference<>();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tableProperties,
            inMemoryStateStoreWithFixedSinglePartition(tableProperties.getSchema()));
    private final IngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();

    @BeforeEach
    public void setUpStepFunctions() {
        when(stepFunctions.startExecution(any(StartExecutionRequest.class)))
                .then((Answer<StartExecutionResult>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return null;
                });
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        instanceProperties.set(BULK_IMPORT_EKS_STATE_MACHINE_ARN, "state-machine-arn");
        instanceProperties.set(BULK_IMPORT_EKS_NAMESPACE, "eks-namespace");
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
    }

    @Test
    void shouldPassJobToStepFunctions() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.job")
                .isEqualTo(new Gson().toJson(myJob));
    }

    @Test
    void shouldPassJobIdToSparkConfig() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.app.name="))
                .containsExactly("spark.app.name=my-job");
    }

    @Test
    void shouldUseDefaultConfigurationIfNoneSpecified() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray()
                .contains("--conf");
    }

    @Test
    void shouldFailValidationWhenInputFilesAreNull() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThat(ingestJobStatusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(myJob.toIngestJob(),
                        rejectedRun(myJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                "The input files must be set to a non-null and non-empty value.")));
    }

    @Test
    void shouldOverwriteDefaultConfigurationIfSpecifiedInJob() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .sparkConf("spark.driver.memory", "10g")
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.driver.memory="))
                .containsExactly("spark.driver.memory=10g");
    }

    @Test
    void shouldIgnoreUserConfigurationIfSetToNull() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .sparkConf("spark.driver.memory", null)
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.driver.memory="))
                .containsExactly("spark.driver.memory=7g");
    }

    @Test
    void shouldUseDefaultJobIdIfNoneWasPresentInTheJob() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.driver.memory="))
                .containsExactly("spark.driver.memory=7g");
    }

    @Test
    void shouldPassConfigBucketAndJobIdsToSparkArgs() {
        // Given
        instanceProperties.set(CONFIG_BUCKET, "myConfigBucket");
        instanceProperties.set(BULK_IMPORT_EKS_STATE_MACHINE_ARN, "myStateMachine");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob, "test-run");

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .endsWith("myConfigBucket", "my-job", "myStateMachine", "test-run", "EKS");
    }

    @Test
    void shouldUseJobIdAsDriverPodName() {
        // Given
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.kubernetes.driver.pod.name="))
                .containsExactly("spark.kubernetes.driver.pod.name=job-my-job");
    }

    @Test
    void shouldTruncateTableNameInStateMachineExecutionName() {
        // Given
        tableProperties.set(TABLE_NAME, "this-is-a-long-table-name-that-will-not-fit-in-an-execution-name-when-combined-with-the-job-id");
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        createExecutorWithDefaults().runJob(myJob);

        // Then
        assertThat(requested.get().getName())
                .isEqualTo("this-is-a-long-table-name-that-will-not-fit-in-an-execution-name-when-com-my-job")
                .hasSize(80);
    }

    @Test
    void shouldNotTruncateTableNameInStateMachineExecutionNameWhenItFits() {
        // Given
        tableProperties.set(TABLE_NAME, "short-table-name");
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        createExecutorWithDefaults().runJob(myJob);

        // Then
        assertThat(requested.get().getName())
                .isEqualTo("short-table-name-my-job");
    }

    @Test
    void shouldFailValidationIfMinimumPartitionCountNotReached() {
        // Given
        tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "5");

        BulkImportExecutor stateMachineExecutor = createExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThat(ingestJobStatusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(myJob.toIngestJob(),
                        rejectedRun(myJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                "The minimum partition count was not reached")));
    }

    private BulkImportExecutor createExecutorWithDefaults() {
        return createExecutorWithValidationTime(Instant.now());
    }

    private BulkImportExecutor createExecutorWithValidationTime(Instant validationTime) {
        return new BulkImportExecutor(instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                stateStoreProvider, ingestJobStatusStore, (job, jobRunId) -> {
                },
                createPlatformExecutor(), List.of(validationTime).iterator()::next);
    }

    private StateMachinePlatformExecutor createPlatformExecutor() {
        return new StateMachinePlatformExecutor(stepFunctions, instanceProperties);
    }

    private BulkImportJob.Builder jobForTable() {
        return BulkImportJob.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME));
    }
}
