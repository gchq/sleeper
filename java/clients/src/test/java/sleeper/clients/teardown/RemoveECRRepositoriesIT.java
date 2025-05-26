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

package sleeper.clients.teardown;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEcrClient;

@WireMockTest
class RemoveECRRepositoriesIT {

    @BeforeEach
    void setUp() {
        stubFor(post("/").willReturn(aResponse().withStatus(200)));
    }

    @Test
    void shouldRemoveRepositories(WireMockRuntimeInfo runtimeInfo) {
        // When
        RemoveECRRepositories.remove(wiremockEcrClient(runtimeInfo), Stream.of("a-repo", "other-repo"));

        // Then
        verify(1, deleteRequestedFor("a-repo"));
        verify(1, deleteRequestedFor("other-repo"));
        verify(2, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldNotThrowAnExceptionWhenRepositoryDoesNotExist(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(post("/").willReturn(repositoryNotFound("test-compaction-repo")));

        // When
        RemoveECRRepositories.remove(wiremockEcrClient(runtimeInfo), Stream.of("test-compaction-repo"));

        // Then
        verify(1, deleteRequestedFor("test-compaction-repo"));
        verify(1, postRequestedFor(urlEqualTo("/")));
    }

    private RequestPatternBuilder deleteRequestedFor(String repositoryName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", matching("^AmazonEC2ContainerRegistry_V\\d+\\.DeleteRepository$"))
                .withRequestBody(matchingJsonPath("$.repositoryName", equalTo(repositoryName))
                        .and(matchingJsonPath("$.force", equalTo("true"))));
    }

    private ResponseDefinitionBuilder repositoryNotFound(String repositoryName) {
        return aResponse()
                .withHeader("Content-Type", "application/json")
                .withHeader("x-amzn-ErrorType", "RepositoryNotFoundException")
                .withBody("{\"message\":\"The repository with name '" + repositoryName + "' " +
                        "does not exist in the registry with id '123'.\"}")
                .withStatus(400);
    }
}
