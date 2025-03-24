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
package sleeper.bulkimport.starter.executor;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sfn.SfnClient;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.starter.executor.BulkImportExecutor.WriteJobToBucket;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.findAll;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.bulkimport.starter.testutil.TestResources.exampleString;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
class StateMachinePlatformExecutorWiremockIT {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateMachinePlatformExecutorWiremockIT.class);

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();

    @BeforeEach
    void setUp() {
        instanceProperties.set(CONFIG_BUCKET, "config-bucket");
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        instanceProperties.set(BULK_IMPORT_EKS_STATE_MACHINE_ARN, "state-machine-arn");
        instanceProperties.set(BULK_IMPORT_EKS_NAMESPACE, "eks-namespace");
        tableProperties.set(TABLE_ID, "table-id");
        tableProperties.set(TABLE_NAME, "test-table");
        transactionLogs.initialiseTable(tableProperties);
    }

    @Test
    void shouldRunAJob(WireMockRuntimeInfo runtimeInfo) {
        // Given
        BulkImportJob job = jobForTable()
                .id("test-job")
                .files(List.of("file.parquet"))
                .build();
        stubStartExecutionIsSuccessful();

        // When
        createExecutor(runtimeInfo).runJob(job, "test-job-run");

        // Then
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("AWSStepFunctions.StartExecution"))))
                .singleElement().extracting(LoggedRequest::getBodyAsString)
                .satisfies(body -> {
                    assertThatJson(body).whenIgnoringPaths("$.input")
                            .isEqualTo(exampleString("example/step-functions/startexecution-request.json"));
                    assertThatJson(body).inPath("$.input").asString()
                            .satisfies(input -> assertThatJson(input)
                                    .isEqualTo(exampleString("example/step-functions/startexecution-input.json")));
                });
    }

    @Test
    void shouldOverwriteDefaultConfigurationIfSpecifiedInJob(WireMockRuntimeInfo runtimeInfo) {
        // Given
        BulkImportJob job = jobForTable()
                .id("my-job")
                .files(List.of("file1.parquet"))
                .sparkConf("spark.driver.memory", "10g")
                .build();
        stubStartExecutionIsSuccessful();

        // When
        createExecutor(runtimeInfo).runJob(job, "test-job-run");

        // Then
        findInputJson(input -> assertThatJson(input)
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.driver.memory="))
                .containsExactly("spark.driver.memory=10g"));
    }

    @Test
    void shouldIgnoreUserConfigurationIfSetToNull(WireMockRuntimeInfo runtimeInfo) {
        // Given
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(List.of("file1.parquet"))
                .sparkConf("spark.driver.memory", null)
                .build();
        stubStartExecutionIsSuccessful();

        // When
        createExecutor(runtimeInfo).runJob(myJob);

        // Then
        findInputJson(input -> assertThatJson(input)
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.driver.memory="))
                .containsExactly("spark.driver.memory=7g"));
    }

    @Test
    void shouldTruncateTableNameInStateMachineExecutionName(WireMockRuntimeInfo runtimeInfo) {
        // Given
        tableProperties.set(TABLE_NAME, "this-is-a-long-table-name-that-will-not-fit-in-an-execution-name-when-combined-with-the-job-id");
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(List.of("file1.parquet"))
                .build();
        stubStartExecutionIsSuccessful();

        // When
        createExecutor(runtimeInfo).runJob(myJob);

        // Then
        findRequestBody(body -> assertThatJson(body)
                .inPath("$.name").asString()
                .isEqualTo("this-is-a-long-table-name-that-will-not-fit-in-an-execution-name-when-com-my-job")
                .hasSize(80));
    }

    @Test
    void shouldNotTruncateTableNameInStateMachineExecutionNameWhenItFits(WireMockRuntimeInfo runtimeInfo) {
        // Given
        tableProperties.set(TABLE_NAME, "short-table-name");
        BulkImportJob myJob = jobForTable()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();
        stubStartExecutionIsSuccessful();

        // When
        createExecutor(runtimeInfo).runJob(myJob);

        // Then
        findRequestBody(body -> assertThatJson(body)
                .inPath("$.name").asString()
                .isEqualTo("short-table-name-my-job"));
    }

    private void stubStartExecutionIsSuccessful() {
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("AWSStepFunctions.StartExecution"))
                .willReturn(aResponse().withStatus(200)));
    }

    private void findInputJson(Consumer<String> assertion) {
        findRequestBody(body -> assertThatJson(body)
                .inPath("$.input").asString().satisfies(assertion));
    }

    private void findRequestBody(Consumer<String> assertion) {
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("AWSStepFunctions.StartExecution"))))
                .singleElement().extracting(LoggedRequest::getBodyAsString)
                .satisfies(assertion);
    }

    private BulkImportExecutor createExecutor(WireMockRuntimeInfo runtimeInfo) {
        return new BulkImportExecutor(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs),
                IngestJobTracker.NONE, noWriteToBucket(),
                createPlatformExecutor(runtimeInfo), Instant::now);
    }

    private WriteJobToBucket noWriteToBucket() {
        return (job, jobRunId) -> {
        };
    }

    private StateMachinePlatformExecutor createPlatformExecutor(WireMockRuntimeInfo runtimeInfo) {
        return new StateMachinePlatformExecutor(
                wiremockAwsV2Client(runtimeInfo, SfnClient.builder()),
                instanceProperties);
    }

    private BulkImportJob.Builder jobForTable() {
        return BulkImportJob.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME));
    }

}
