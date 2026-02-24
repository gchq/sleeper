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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent.CloudFormationCustomResourceEventBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.cdk.custom.testutil.FakeLambdaContext;
import sleeper.cdk.custom.testutil.NexusRepositoryContainer;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@Testcontainers
@WireMockTest
public class CopyJarLambdaIT extends LocalStackTestBase {

    @Container
    public final NexusRepositoryContainer source = new NexusRepositoryContainer();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(JARS_BUCKET));
        stubFor(put("/report-response").willReturn(aResponse().withStatus(200)));
    }

    @Test
    @Disabled("TODO")
    void shouldCopyJarFromMavenToBucket(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        source.createRepository("test-repository");
        String url = source.uploadJarGetUrl("test-repository", "test-artifact", "some-content");
        source.listComponents("test-repository");

        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Create")
                .withResourceProperties(Map.of(
                        "source", url,
                        "version", "4.2",
                        "artifactId", "cdk-custom-resources"))
                .build());

        // Then
        assertThat(listObjectKeys(instanceProperties.get(JARS_BUCKET)))
                .containsExactly("cdk-custom-resources-4.2.jar");
        assertThat(getObjectAsString(instanceProperties.get(JARS_BUCKET), "cdk-custom-resources-4.2.jar"))
                .isEqualTo("some-content");
        verify(putRequestedFor(urlEqualTo("/report-response"))
                .withRequestBody(matchingJsonPath("$.Data.versionId", matching("[a-z0-9]+"))
                        .and(matchingJsonPath("$.Status", equalTo("SUCCESS")))));
    }

    private void handleEvent(CloudFormationCustomResourceEvent event) throws Exception {
        lambda().handleRequest(event, new FakeLambdaContext());
    }

    private CloudFormationCustomResourceEventBuilder event(WireMockRuntimeInfo runtimeInfo) {
        return CloudFormationCustomResourceEvent.builder()
                .withResponseUrl(runtimeInfo.getHttpBaseUrl() + "/report-response");
    }

    private CopyJarLambda lambda() {
        return new CopyJarLambda();
    }
}
