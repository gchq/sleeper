/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.rejectedRun;

class StateMachinePlatformExecutorTest {
    private AWSStepFunctions stepFunctions;
    private InstanceProperties instanceProperties;
    private TablePropertiesProvider tablePropertiesProvider;
    private StateStoreProvider stateStoreProvider;
    private AtomicReference<StartExecutionRequest> requested;
    private AmazonS3 amazonS3;
    private IngestJobStatusStore ingestJobStatusStore;

    @BeforeEach
    public void setUpStepFunctions() {
        requested = new AtomicReference<>();
        stepFunctions = mock(AWSStepFunctions.class);
        amazonS3 = mock(AmazonS3.class);
        when(stepFunctions.startExecution(any(StartExecutionRequest.class)))
                .then((Answer<StartExecutionResult>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return null;
                });
        instanceProperties = new InstanceProperties();
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        tablePropertiesProvider = mock(TablePropertiesProvider.class);
        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> new TableProperties(instanceProperties));
        stateStoreProvider = mock(StateStoreProvider.class);
        when(stateStoreProvider.getStateStore(any())).thenReturn(
                inMemoryStateStoreWithFixedSinglePartition(schemaWithKey("key")));
        ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
    }

    @Test
    void shouldPassJobToStepFunctions() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
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
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
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
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
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
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        BulkImportExecutor stateMachineExecutor = createExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThat(ingestJobStatusStore.getAllJobs("myTable"))
                .containsExactly(jobStatus(myJob.toIngestJob(),
                        rejectedRun(myJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                "The input files must be set to a non-null and non-empty value.")));
    }

    @Test
    void shouldOverwriteDefaultConfigurationIfSpecifiedInJob() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
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
    void shouldUseDefaultJobIdIfNoneWasPresentInTheJob() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
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
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        instanceProperties.set(BULK_IMPORT_EKS_STATE_MACHINE_ARN, "myStateMachine");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob, "test-run");

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .endsWith("myConfigBucket", "my-job", "myStateMachine", "test-run");
    }

    @Test
    void shouldUseJobIdAsDriverPodName() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        BulkImportExecutor stateMachineExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
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
    void shouldFailValidationIfMinimumPartitionCountNotReached() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        when(tablePropertiesProvider.getTableProperties(any()))
                .thenAnswer((Answer<TableProperties>) x -> {
                    TableProperties tableProperties = new TableProperties(instanceProperties);
                    tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "5");
                    return tableProperties;
                });

        BulkImportExecutor stateMachineExecutor = createExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThat(ingestJobStatusStore.getAllJobs("myTable"))
                .containsExactly(jobStatus(myJob.toIngestJob(),
                        rejectedRun(myJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                "The minimum partition count was not reached")));
    }

    private BulkImportExecutor createExecutorWithDefaults() {
        return createExecutorWithValidationTime(Instant.now());
    }

    private BulkImportExecutor createExecutorWithValidationTime(Instant validationTime) {
        return new BulkImportExecutor(instanceProperties, tablePropertiesProvider,
                stateStoreProvider, ingestJobStatusStore, amazonS3,
                createPlatformExecutor(), List.of(validationTime).iterator()::next);
    }

    private StateMachinePlatformExecutor createPlatformExecutor() {
        return new StateMachinePlatformExecutor(stepFunctions, instanceProperties);
    }
}
