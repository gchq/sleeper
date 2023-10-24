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

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.bulkimport.starter.testutil.TestResources.exampleString;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;

@WireMockTest
public class EmrServerlessPlatformExecutorIT {

    public static final String WIREMOCK_ACCESS_KEY = "wiremock-access-key";
    public static final String WIREMOCK_SECRET_KEY = "wiremock-secret-key";

    private final InstanceProperties properties = createTestInstanceProperties();
    private final TablePropertiesProvider tableProperties = mock(TablePropertiesProvider.class);

    @Test
    void shouldRunAServerlessJob(WireMockRuntimeInfo runtimeInfo) {
        properties.set(ID, "instance");
        properties.set(CONFIG_BUCKET, "config-bucket");
        properties.set(BULK_IMPORT_BUCKET, "import-bucket");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME, "cluster-name");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID, "application-id");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, "cluster-role");
        properties.set(BULK_IMPORT_CLASS_NAME, "BulkImportClass");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(properties)
                .bulkImportJob(job).jobRunId("run-id")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));
        when(tableProperties.getByName(anyString())).thenReturn(createTestTableProperties(properties, null));

        new EmrServerlessPlatformExecutor(wiremockEmrClient(runtimeInfo), properties, tableProperties)
                .runJobOnPlatform(arguments);

        verify(postRequestedFor(urlEqualTo("/applications/application-id/jobruns"))
                .withRequestBody(equalToJson(
                        exampleString("example/emr-serverless/jobrun-request.json"), false, true))
                .withRequestBody(matchingJsonPath("$.jobDriver.sparkSubmit.sparkSubmitParameters",
                        containing("--class BulkImportClass"))));
    }

    @Test
    void shouldRunAServerlessJobWithJobSparkConfSet(WireMockRuntimeInfo runtimeInfo) {
        properties.set(ID, "instance");
        properties.set(CONFIG_BUCKET, "config-bucket");
        properties.set(BULK_IMPORT_BUCKET, "import-bucket");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME, "cluster-name");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID, "application-id");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, "cluster-role");
        properties.set(BULK_IMPORT_CLASS_NAME, "BulkImportClass");
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .sparkConf(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK.getPropertyName(), "100G")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(properties)
                .bulkImportJob(job).jobRunId("run-id")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));
        when(tableProperties.getByName(anyString())).thenReturn(createTestTableProperties(properties, null));

        new EmrServerlessPlatformExecutor(wiremockEmrClient(runtimeInfo), properties, tableProperties)
                .runJobOnPlatform(arguments);

        verify(postRequestedFor(urlEqualTo("/applications/application-id/jobruns"))
                .withRequestBody(equalToJson(
                        exampleString("example/emr-serverless/jobrun-request.json"), false, true))
                .withRequestBody(matchingJsonPath("$.jobDriver.sparkSubmit.sparkSubmitParameters",
                        containing("spark.emr-serverless.executor.disk=100G"))));
    }

    private static EmrServerlessClient wiremockEmrClient(WireMockRuntimeInfo runtimeInfo) {
        return EmrServerlessClient.builder()
                .endpointOverride(wiremockEndpointOverride(runtimeInfo))
                .credentialsProvider(wiremockCredentialsProvider())
                .region(Region.AWS_GLOBAL)
                .build();
    }

    private static URI wiremockEndpointOverride(WireMockRuntimeInfo runtimeInfo) {
        try {
            return new URI(runtimeInfo.getHttpBaseUrl());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static AwsCredentialsProvider wiremockCredentialsProvider() {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(WIREMOCK_ACCESS_KEY, WIREMOCK_SECRET_KEY));
    }
}
