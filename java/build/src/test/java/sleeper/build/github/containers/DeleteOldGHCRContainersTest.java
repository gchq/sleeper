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
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubApi;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubRequest;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubResponse;
import static sleeper.build.github.api.TestGitHubJson.gitHubJson;
import static sleeper.build.github.containers.TestGitHubPackage.packageWithName;
import static sleeper.build.github.containers.TestGitHubVersion.versionWithId;
import static sleeper.build.testutil.TestResources.exampleString;

@WireMockTest
class DeleteOldGHCRContainersTest {

    @Test
    void shouldDeleteAContainerVersion(WireMockRuntimeInfo runtimeInfo) {
        // Given
        containerListReturns(exampleString("examples/github-api/package-list-one-container.json"));
        packageVersionListReturns("sleeper-local",
                exampleString("examples/github-api/package-version-list-one-image.json"));
        packageVersionDeleteSucceeds("sleeper-local", 64403175);

        // When
        deleteAll(runtimeInfo);

        // Then
        verify(packageVersionDeleted("sleeper-local", 64403175));
    }

    @Test
    void shouldNotDeleteSpecifiedTag(WireMockRuntimeInfo runtimeInfo) {
        // Given
        containerListReturns(packageWithName("sleeper-local"));

        // TODO add tag
        packageVersionListReturns("sleeper-local", versionWithId(123));
        packageVersionDeleteSucceeds("sleeper-local", 123);

        // When
        deleteAll(runtimeInfo);

        // Then
        verify(packageVersionDeleted("sleeper-local", 123));
    }

    private void deleteAll(WireMockRuntimeInfo runtimeInfo) {
        new DeleteGHCRImages(gitHubApi(runtimeInfo), "test-org").deleteAll();
    }

    private void containerListReturns(TestGitHubPackage... packages) {
        containerListReturns(gitHubJson(List.of(packages)));
    }

    private void containerListReturns(String body) {
        stubFor(gitHubRequest(get("/orgs/test-org/packages?package_type=container"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(body)));
    }

    private void packageVersionListReturns(String packageName, TestGitHubVersion... versions) {
        packageVersionListReturns(packageName, gitHubJson(List.of(versions)));
    }

    private void packageVersionListReturns(String packageName, String body) {
        stubFor(gitHubRequest(get("/orgs/test-org/packages/container/" + packageName + "/versions"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(body)));
    }

    private void packageVersionDeleteSucceeds(String packageName, int versionId) {
        stubFor(gitHubRequest(delete("/orgs/test-org/packages/container/" + packageName + "/versions/" + versionId))
                .willReturn(gitHubResponse().withStatus(204)));
    }

    private RequestPatternBuilder packageVersionDeleted(String packageName, int versionId) {
        return gitHubRequest(deleteRequestedFor(
                urlEqualTo("/orgs/test-org/packages/container/" + packageName + "/versions/" + versionId)));
    }
}
