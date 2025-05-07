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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.emr.EmrClient;

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
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_TYPE;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_CORES;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY_OVERHEAD;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY_OVERHEAD;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_RDD_COMPRESS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
class EmrPlatformExecutorWiremockIT {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();

    @BeforeEach
    void setUp() {
        instanceProperties.set(ID, "test-instance");
        instanceProperties.set(CONFIG_BUCKET, "config-bucket");
        instanceProperties.set(JARS_BUCKET, "jars-bucket");
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY, "16g");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY, "16g");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES, "29");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY_OVERHEAD, "1706m");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY_OVERHEAD, "1706m");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM, "290");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS, "290");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES, "5");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DRIVER_CORES, "5");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT, "800s");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL, "60s");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED, "false");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION, "0.80");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION, "0.30");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS,
                "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS,
                "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES, "5");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL, "MEMORY_AND_DISK_SER");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_RDD_COMPRESS, "true");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS, "true");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS, "true");
        instanceProperties.set(BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB, "256");
        instanceProperties.set(BULK_IMPORT_EMR_EBS_VOLUME_TYPE, "gp2");
        instanceProperties.set(BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE, "4");
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
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.RunJobFlow"))
                .willReturn(aResponse().withStatus(200)));

        // When
        createExecutor(runtimeInfo).runJob(job, "test-run");

        // Then
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.RunJobFlow"))))
                .singleElement()
                .satisfies(request -> assertThatJson(request.getBodyAsString())
                        .isEqualTo(exampleString("example/emr/runjobflow-request.json")));
    }

    private BulkImportExecutor createExecutor(WireMockRuntimeInfo runtimeInfo) {
        return new BulkImportExecutor(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs),
                IngestJobTracker.NONE, noWriteToBucket(),
                createPlatformExecutor(runtimeInfo), Instant::now);
    }

    private EmrPlatformExecutor createPlatformExecutor(WireMockRuntimeInfo runtimeInfo) {
        return new EmrPlatformExecutor(
                wiremockAwsV2Client(runtimeInfo, EmrClient.builder()
                        .overrideConfiguration(config -> config
                                .retryStrategy(retry -> retry
                                        .maxAttempts(2)
                                        .backoffStrategy(BackoffStrategy.retryImmediately())
                                        .throttlingBackoffStrategy(BackoffStrategy.retryImmediately())))),
                instanceProperties, new FixedTablePropertiesProvider(tableProperties));
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
