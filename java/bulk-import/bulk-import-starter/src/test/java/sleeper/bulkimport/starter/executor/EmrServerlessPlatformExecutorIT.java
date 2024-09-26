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

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

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
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@WireMockTest
public class EmrServerlessPlatformExecutorIT {

    public static final String WIREMOCK_ACCESS_KEY = "wiremock-access-key";
    public static final String WIREMOCK_SECRET_KEY = "wiremock-secret-key";

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        instanceProperties.set(ID, "instance");
        instanceProperties.set(CONFIG_BUCKET, "config-bucket");
        instanceProperties.set(BULK_IMPORT_BUCKET, "import-bucket");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME, "my-application");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID, "application-id");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, "cluster-role");
        instanceProperties.set(BULK_IMPORT_CLASS_NAME, "BulkImportClass");
    }

    @Test
    void shouldRunAServerlessJob(WireMockRuntimeInfo runtimeInfo) {
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(job).jobRunId("run-id")
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
                            .contains(" --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64 ");
                });
    }

    @Test
    void shouldRunAServerlessJobWithJobSparkConfSet(WireMockRuntimeInfo runtimeInfo) {
        BulkImportJob job = BulkImportJob.builder()
                .id("my-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .sparkConf("spark.emr-serverless.executor.disk", "100G")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(job).jobRunId("run-id")
                .build();
        stubFor(post("/applications/application-id/jobruns").willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments);

        verify(postRequestedFor(urlEqualTo("/applications/application-id/jobruns"))
                .withRequestBody(equalToJson(
                        exampleString("example/emr-serverless/jobrun-request.json"), false, true))
                .withRequestBody(matchingJsonPath("$.jobDriver.sparkSubmit.sparkSubmitParameters",
                        containing(" --conf spark.emr-serverless.executor.disk=100G "))));
    }

    private EmrServerlessPlatformExecutor executor(WireMockRuntimeInfo runtimeInfo) {
        return new EmrServerlessPlatformExecutor(wiremockEmrClient(runtimeInfo), instanceProperties);
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
