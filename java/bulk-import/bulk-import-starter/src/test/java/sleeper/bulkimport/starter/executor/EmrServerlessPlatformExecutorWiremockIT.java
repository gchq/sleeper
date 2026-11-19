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

package sleeper.bulkimport.starter.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import net.javacrumbs.jsonunit.assertj.JsonAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.EmrInstanceArchitecture;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.findAll;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.bulkimport.starter.testutil.TestResources.exampleString;
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_OPTIMIZATION;
import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION;
import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.local.aws.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
public class EmrServerlessPlatformExecutorWiremockIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));

    @BeforeEach
    void setUp() {
        instanceProperties.set(ID, "instance");
        instanceProperties.set(CONFIG_BUCKET, "config-bucket");
        instanceProperties.set(JARS_BUCKET, "jars-bucket");
        instanceProperties.set(BULK_IMPORT_BUCKET, "import-bucket");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME, "my-application");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID, "application-id");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, "cluster-role");
        instanceProperties.set(BULK_IMPORT_CLASS_NAME, "BulkImportClass");
        tableProperties.set(TABLE_NAME, "table-name");
    }

    @Test
    void shouldRunAServerlessJob(WireMockRuntimeInfo runtimeInfo) throws Exception {
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(job).jobRunId("run-id")
                .jobFileObjectKey("folder/job.json")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments);

        assertThat(findAll(postRequestedFor(urlEqualTo("/applications/application-id/jobruns"))))
                .singleElement().extracting(LoggedRequest::getBodyAsString)
                .satisfies(body -> {
                    assertThatJson(body)
                            .whenIgnoringPaths(
                                    "$.clientToken",
                                    "$.jobDriver.sparkSubmit.sparkSubmitParameters")
                            .isEqualTo(exampleString("example/emr-serverless/jobrun-request.json"));
                    assertThatJson(body)
                            .inPath("$.jobDriver.sparkSubmit.sparkSubmitParameters").asString()
                            .startsWith("--class BulkImportClass ")
                            .contains(" --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.aarch64 ");
                });
    }

    @Test
    void shouldRunAServerlessJobOnX86(WireMockRuntimeInfo runtimeInfo) {
        instanceProperties.setEnum(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE, EmrInstanceArchitecture.X86_64);
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(job).jobRunId("run-id")
                .jobFileObjectKey("folder/job.json")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments);

        assertThat(findAll(postRequestedFor(urlEqualTo("/applications/application-id/jobruns"))))
                .singleElement().extracting(LoggedRequest::getBodyAsString)
                .satisfies(body -> assertThatJson(body)
                        .inPath("$.jobDriver.sparkSubmit.sparkSubmitParameters").asString()
                        .contains(" --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 ")
                        .contains(" --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 ")
                        .doesNotContain("aarch64"));
    }

    @Test
    void shouldRunAServerlessJobWithJobSparkConfSet(WireMockRuntimeInfo runtimeInfo) throws Exception {
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .sparkConf("spark.emr-serverless.executor.disk", "100G")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(job).jobRunId("run-id")
                .jobFileObjectKey("folder/job.json")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments);

        verify(postRequestedFor(urlEqualTo("/applications/application-id/jobruns"))
                .withRequestBody(equalToJson(
                        exampleString("example/emr-serverless/jobrun-request.json"), false, true))
                .withRequestBody(matchingJsonPath("$.jobDriver.sparkSubmit.sparkSubmitParameters",
                        containing(" --conf spark.emr-serverless.executor.disk=100G "))));
    }

    @Test
    void shouldSetSparkConfFromTableProperty(WireMockRuntimeInfo runtimeInfo) throws Exception {
        tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK, "150G");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.emr-serverless.executor.disk=150G ");
    }

    @Test
    void shouldSetSparkConfFromInstanceDefaultWhenTablePropertyUnset(WireMockRuntimeInfo runtimeInfo) throws Exception {
        instanceProperties.set(DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION, "false");
        instanceProperties.set(DEFAULT_BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES, "12");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.executor.instances=12 ");
    }

    @Test
    void shouldOverrideTablePropertyWithJobPlatformSpec(WireMockRuntimeInfo runtimeInfo) throws Exception {
        tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK, "150G");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .platformSpec(Map.of(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK.getPropertyName(), "50G"))
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.emr-serverless.executor.disk=50G ")
                .doesNotContain("150G");
    }

    @Test
    void shouldUseDynamicAllocationByDefault(WireMockRuntimeInfo runtimeInfo) throws Exception {
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.dynamicAllocation.enabled=true ")
                .contains(" --conf spark.dynamicAllocation.minExecutors=0 ")
                .contains(" --conf spark.dynamicAllocation.initialExecutors=3 ")
                .contains(" --conf spark.dynamicAllocation.maxExecutors=36 ")
                .contains(" --conf spark.dynamicAllocation.executorIdleTimeout=60s ")
                .doesNotContain("spark.executor.instances");
        assertThatApplicationConfiguration().isArray().singleElement().isEqualTo("""
                {"classification": "spark", "properties": {"dynamicAllocationOptimization": "true"}}""");
    }

    @Test
    void shouldUseFixedExecutorsWhenDynamicAllocationDisabledForTable(WireMockRuntimeInfo runtimeInfo) throws Exception {
        tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION, "false");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.dynamicAllocation.enabled=false ")
                .contains(" --conf spark.executor.instances=36 ")
                .doesNotContain("Executors=")
                .doesNotContain("executorIdleTimeout");
        assertThatApplicationConfiguration().isAbsent();
    }

    @Test
    void shouldSetDynamicAllocationExecutorBoundsFromTableProperties(WireMockRuntimeInfo runtimeInfo) throws Exception {
        tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, "1");
        tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, "2");
        tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "10");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.dynamicAllocation.minExecutors=1 ")
                .contains(" --conf spark.dynamicAllocation.initialExecutors=2 ")
                .contains(" --conf spark.dynamicAllocation.maxExecutors=10 ");
    }

    @Test
    void shouldOverrideDynamicAllocationMaxExecutorsWithJobPlatformSpec(WireMockRuntimeInfo runtimeInfo) throws Exception {
        tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "10");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .platformSpec(Map.of(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS.getPropertyName(), "50"))
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.dynamicAllocation.maxExecutors=50 ");
    }

    @Test
    void shouldNotSetDynamicAllocationOptimizationWhenDisabledForInstance(WireMockRuntimeInfo runtimeInfo) throws Exception {
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_OPTIMIZATION, "false");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments(job));

        assertThatSparkSubmitParameters()
                .contains(" --conf spark.dynamicAllocation.enabled=true ");
        assertThatApplicationConfiguration().isAbsent();
    }

    private BulkImportArguments arguments(BulkImportJob job) {
        return BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(job).jobRunId("run-id")
                .jobFileObjectKey("folder/job.json")
                .build();
    }

    private AbstractStringAssert<?> assertThatSparkSubmitParameters() throws Exception {
        List<LoggedRequest> requests = findAll(postRequestedFor(urlEqualTo("/applications/application-id/jobruns")));
        assertThat(requests).hasSize(1);
        String parameters = new ObjectMapper().readTree(requests.get(0).getBodyAsString())
                .at("/jobDriver/sparkSubmit/sparkSubmitParameters").asText();
        // A trailing space is added so that every --conf entry can be matched with a space after it, including the last
        return assertThat(parameters + " ");
    }

    private JsonAssert assertThatApplicationConfiguration() {
        List<LoggedRequest> requests = findAll(postRequestedFor(urlEqualTo("/applications/application-id/jobruns")));
        assertThat(requests).hasSize(1);
        return assertThatJson(requests.get(0).getBodyAsString())
                .node("configurationOverrides.applicationConfiguration");
    }

    private EmrServerlessPlatformExecutor executor(WireMockRuntimeInfo runtimeInfo) {
        return new EmrServerlessPlatformExecutor(wiremockAwsV2Client(runtimeInfo, EmrServerlessClient.builder()),
                instanceProperties, new FixedTablePropertiesProvider(tableProperties));
    }
}
