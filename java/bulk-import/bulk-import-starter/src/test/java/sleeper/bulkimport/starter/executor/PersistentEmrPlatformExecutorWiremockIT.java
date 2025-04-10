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
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emr.EmrClient;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.starter.executor.BulkImportExecutor.WriteJobToBucket;
import sleeper.bulkimport.starter.executor.persistent.PersistentEmrPlatformExecutor;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2ClientWithRetryAttempts;

@WireMockTest
class PersistentEmrPlatformExecutorWiremockIT {
    public static final Logger LOGGER = LoggerFactory.getLogger(PersistentEmrPlatformExecutorWiremockIT.class);

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    List<BulkImportJob> jobsReturnedToQueue = new ArrayList<>();

    @BeforeEach
    void setUp() {
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        instanceProperties.set(CONFIG_BUCKET, "test-config-bucket");
        instanceProperties.set(JARS_BUCKET, "test-jars-bucket");
        instanceProperties.set(VERSION, "1.2.3");
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-cluster");
        instanceProperties.set(BULK_IMPORT_CLASS_NAME, "BulkImportClass");
        tableProperties.set(TABLE_ID, "table-id");
        tableProperties.set(TABLE_NAME, "table-name");
        transactionLogs.initialiseTable(tableProperties);
    }

    @Test
    void shouldRunAJob(WireMockRuntimeInfo runtimeInfo) {
        // Given
        BulkImportJob job = jobForTable()
                .id("test-job")
                .files(List.of("file.parquet"))
                .build();
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))
                .willReturn(aResponse().withStatus(200)
                        .withBody(exampleString("example/persistent-emr/listclusters-response.json"))));
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))
                .willReturn(aResponse().withStatus(200)));

        // When
        createExecutor(runtimeInfo).runJob(job, "test-run");

        // Then
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))))
                .singleElement()
                .satisfies(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo("{\"ClusterStates\":[\"BOOTSTRAPPING\",\"RUNNING\",\"STARTING\",\"WAITING\"]}"));
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))))
                .singleElement()
                .satisfies(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo(exampleString("example/persistent-emr/addjobflow-request.json")));
    }

    @Test
    void shouldRetryWhenRateLimitedOnListClusters(WireMockRuntimeInfo runtimeInfo) {
        // Given
        BulkImportJob job = jobForTable()
                .id("test-job")
                .files(List.of("file.parquet"))
                .build();
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))
                .inScenario("retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("retry")
                .willReturn(aResponse().withStatus(400)
                        .withHeader("X-Amzn-ErrorType", "ThrottlingException")
                        .withHeader("X-Amzn-Error-Message", "Rate limit exceeded")));
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))
                .inScenario("retry")
                .whenScenarioStateIs("retry")
                .willReturn(aResponse().withStatus(200)
                        .withBody(exampleString("example/persistent-emr/listclusters-response.json"))));
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))
                .willReturn(aResponse().withStatus(200)));

        // When
        createExecutor(runtimeInfo).runJob(job, "test-run");

        // Then
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))))
                .hasSize(2)
                .allSatisfy(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo("{\"ClusterStates\":[\"BOOTSTRAPPING\",\"RUNNING\",\"STARTING\",\"WAITING\"]}"));
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))))
                .singleElement()
                .satisfies(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo(exampleString("example/persistent-emr/addjobflow-request.json")));
    }

    @Test
    void shouldRetryWhenRateLimitedOnAddJob(WireMockRuntimeInfo runtimeInfo) {
        // Given
        BulkImportJob job = jobForTable()
                .id("test-job")
                .files(List.of("file.parquet"))
                .build();
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))
                .willReturn(aResponse().withStatus(200)
                        .withBody(exampleString("example/persistent-emr/listclusters-response.json"))));
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))
                .inScenario("retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("retry")
                .willReturn(aResponse().withStatus(400)
                        .withHeader("X-Amzn-ErrorType", "ThrottlingException")
                        .withHeader("X-Amzn-Error-Message", "Rate limit exceeded")));
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))
                .inScenario("retry")
                .whenScenarioStateIs("retry")
                .willReturn(aResponse().withStatus(200)));

        // When
        createExecutor(runtimeInfo).runJob(job, "test-run");

        // Then
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))))
                .singleElement()
                .satisfies(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo("{\"ClusterStates\":[\"BOOTSTRAPPING\",\"RUNNING\",\"STARTING\",\"WAITING\"]}"));
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))))
                .hasSize(2)
                .allSatisfy(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo(exampleString("example/persistent-emr/addjobflow-request.json")));
    }

    @Test
    void shouldReturnToQueueWhenClusterIsFull(WireMockRuntimeInfo runtimeInfo) {
        // Given
        BulkImportJob job = jobForTable()
                .id("test-job")
                .files(List.of("file.parquet"))
                .build();
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))
                .willReturn(aResponse().withStatus(200)
                        .withBody(exampleString("example/persistent-emr/listclusters-response.json"))));
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))
                .willReturn(aResponse().withStatus(400)
                        .withBody(exampleString("example/persistent-emr/addjobflow-error-cluster-full.json"))));

        // When
        createExecutor(runtimeInfo).runJob(job, "test-run");

        // Then
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))))
                .singleElement()
                .satisfies(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo(exampleString("example/persistent-emr/addjobflow-request.json")));
        assertThat(jobsReturnedToQueue).containsExactly(job);
    }

    private BulkImportExecutor createExecutor(WireMockRuntimeInfo runtimeInfo) {
        return new BulkImportExecutor(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs),
                IngestJobTracker.NONE, noWriteToBucket(),
                createPlatformExecutor(runtimeInfo), Instant::now);
    }

    private PersistentEmrPlatformExecutor createPlatformExecutor(WireMockRuntimeInfo runtimeInfo) {
        return new PersistentEmrPlatformExecutor(
                wiremockAwsV2ClientWithRetryAttempts(2, runtimeInfo, EmrClient.builder()),
                jobsReturnedToQueue::add,
                instanceProperties);
    }

    private WriteJobToBucket noWriteToBucket() {
        return (job, jobRunId) -> {
        };
    }

    private BulkImportJob.Builder jobForTable() {
        return BulkImportJob.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME));
    }

}
