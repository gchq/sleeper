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

package sleeper.build.github.containers;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubRequest;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubResponse;
import static sleeper.build.testutil.TestResources.exampleString;

@WireMockTest
class DeleteOldGHCRContainersTest {
    @Test
    @Disabled("TODO")
    void shouldDeleteAContainerVersion(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(gitHubRequest(get("/orgs/test-org/packages?package_type=container"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(exampleString("examples/github-api/package-list-one-container.json"))));
        stubFor(gitHubRequest(get("/orgs/test-org/packages/container/sleeper-local/versions"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(exampleString("examples/github-api/package-version-list-one-image.json"))));

        // When
        deleteAllContainers(runtimeInfo);

        // Then
        verify(gitHubRequest(deleteRequestedFor(
                urlEqualTo("/orgs/test-org/packages/container/sleeper-local/versions/64403175"))));
    }

    private void deleteAllContainers(WireMockRuntimeInfo runtimeInfo) {

    }
}
