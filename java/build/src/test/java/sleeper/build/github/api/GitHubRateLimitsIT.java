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
package sleeper.build.github.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubRequest;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubResponse;
import static sleeper.build.github.api.GitHubApiTestHelper.returnWithGitHubApi;
import static sleeper.build.testutil.TestResources.exampleString;

@WireMockTest
class GitHubRateLimitsIT {

    @Test
    void shouldGetExampleRateLimits(WireMockRuntimeInfo runtimeInfo) {
        stubFor(gitHubRequest(get("/rate_limit"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(exampleString("examples/github-api/rate-limit.json"))));

        JsonNode response = returnWithGitHubApi(runtimeInfo, GitHubRateLimits::get);

        assertThat(GitHubRateLimits.remainingLimit(response)).isEqualTo(4999);
        assertThat(GitHubRateLimits.resetTime(response)).isEqualTo(Instant.parse("2013-07-01T17:47:53Z"));
    }
}
