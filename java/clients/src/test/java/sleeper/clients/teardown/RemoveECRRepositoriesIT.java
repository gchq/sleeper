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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.DummyInstanceProperty;
import sleeper.configuration.properties.InstanceProperties;

import java.util.List;

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
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;

@WireMockTest
class RemoveECRRepositoriesIT {

    @BeforeEach
    void setUp() {
        stubFor(post("/").willReturn(aResponse().withStatus(200)));
    }

    @Test
    void shouldRemoveRepositoryWhenPropertyIsSet(WireMockRuntimeInfo runtimeInfo) {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ECR_COMPACTION_REPO, "test-compaction-repo");

        // When
        RemoveECRRepositories.remove(wiremockEcrClient(runtimeInfo), properties, List.of());

        // Then
        verify(1, deleteRequestedFor("test-compaction-repo"));
        verify(1, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldRemoveRepositoriesWhenAllPropertiesAreSet(WireMockRuntimeInfo runtimeInfo) {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ECR_COMPACTION_REPO, "test-compaction-repo");
        properties.set(ECR_INGEST_REPO, "test-ingest-repo");
        properties.set(BULK_IMPORT_REPO, "test-bulk-import-repo");

        // When
        RemoveECRRepositories.remove(wiremockEcrClient(runtimeInfo), properties, List.of());

        // Then
        verify(1, deleteRequestedFor("test-compaction-repo"));
        verify(1, deleteRequestedFor("test-ingest-repo"));
        verify(1, deleteRequestedFor("test-bulk-import-repo"));
        verify(3, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldRemoveExtraRepositoriesWhenSetInProperties(WireMockRuntimeInfo runtimeInfo) {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        DummyInstanceProperty extraRepository = new DummyInstanceProperty("extra.repo");
        properties.set(extraRepository, "test-extra-repo");

        // When
        RemoveECRRepositories.remove(wiremockEcrClient(runtimeInfo), properties, List.of(extraRepository));

        // Then
        verify(1, deleteRequestedFor("test-extra-repo"));
        verify(1, postRequestedFor(urlEqualTo("/")));
    }

    private RequestPatternBuilder deleteRequestedFor(String repositoryName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", matching("^AmazonEC2ContainerRegistry_V\\d+\\.DeleteRepository$"))
                .withRequestBody(equalToJson("{\"repositoryName\":\"" + repositoryName + "\"}"));
    }
}
