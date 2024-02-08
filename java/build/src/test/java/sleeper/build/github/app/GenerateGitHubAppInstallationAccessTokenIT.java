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

package sleeper.build.github.app;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubRequest;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubResponse;
import static sleeper.build.github.api.GitHubApiTestHelper.returnWithGitHubApi;
import static sleeper.build.testutil.TestResources.exampleString;

@WireMockTest
public class GenerateGitHubAppInstallationAccessTokenIT {

    @Test
    void shouldGetInstallationAccessToken(WireMockRuntimeInfo runtimeInfo) {
        stubFor(gitHubRequest(post("/app/installations/test-installation/access_tokens"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(exampleString("examples/github-api/installation-access-token-response.json"))));

        String accessToken = returnWithGitHubApi(runtimeInfo, api -> new GenerateGitHubAppInstallationAccessToken(api)
                .generateWithInstallationId("test-installation"));

        assertThat(accessToken).isEqualTo("ghs_16C7e42F292c6912E7710c838347Ae178B4a");
    }
}
