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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emr.EmrClient;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;

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
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
public class PersistentEmrPlatformExecutorWiremockIT {
    public static final Logger LOGGER = LoggerFactory.getLogger(PersistentEmrPlatformExecutorWiremockIT.class);

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        instanceProperties.set(CONFIG_BUCKET, "test-config-bucket");
        instanceProperties.set(JARS_BUCKET, "test-jars-bucket");
        instanceProperties.set(VERSION, "1.2.3");
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-cluster");
        instanceProperties.set(BULK_IMPORT_CLASS_NAME, "BulkImportClass");
    }

    @Test
    void shouldRunAJob(WireMockRuntimeInfo runtimeInfo) {
        BulkImportJob job = BulkImportJob.builder()
                .id("test-job")
                .files(List.of("file.parquet"))
                .tableName("table-name")
                .build();
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(job).jobRunId("test-run")
                .build();
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))
                .willReturn(aResponse().withStatus(200)
                        .withBody(exampleString("example/persistent-emr/listclusters-response.json"))));
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))
                .willReturn(aResponse().withStatus(200)));

        executor(runtimeInfo).runJobOnPlatform(arguments);

        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.ListClusters"))))
                .singleElement().extracting(LoggedRequest::getBodyAsString)
                .satisfies(body -> assertThatJson(body)
                        .isEqualTo("{\"ClusterStates\":[\"BOOTSTRAPPING\",\"RUNNING\",\"STARTING\",\"WAITING\"]}"));
        assertThat(findAll(postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("ElasticMapReduce.AddJobFlowSteps"))))
                .singleElement().extracting(LoggedRequest::getBodyAsString)
                .satisfies(body -> assertThatJson(body)
                        .isEqualTo(exampleString("example/persistent-emr/addjobflow-request.json")));
    }

    private PersistentEmrPlatformExecutor executor(WireMockRuntimeInfo runtimeInfo) {
        return new PersistentEmrPlatformExecutor(wiremockAwsV2Client(runtimeInfo, EmrClient.builder()), instanceProperties);
    }

}
