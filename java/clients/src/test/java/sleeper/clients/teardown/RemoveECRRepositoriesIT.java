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

package sleeper.clients.teardown;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.WiremockTestHelper.wiremockEcrClient;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;

@WireMockTest
class RemoveECRRepositoriesIT {
    @Test
    void shouldRemoveRepositoryWhenPropertyIsSet(WireMockRuntimeInfo runtimeInfo) {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ECR_COMPACTION_REPO, "test-compaction-repo");
        stubFor(post("/").willReturn(aResponse().withStatus(200)));

        // When
        RemoveECRRepositories.remove(wiremockEcrClient(runtimeInfo), properties);

        // Then
        verify(1, deleteRequestedFor("test-compaction-repo"));
    }

    private RequestPatternBuilder deleteRequestedFor(String repositoryName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", matching("^AmazonEC2ContainerRegistry_V\\d+\\.DeleteRepository$"))
                .withRequestBody(equalToJson("{\"repositoryName\":\"" + repositoryName + "\"}"));
    }
}
