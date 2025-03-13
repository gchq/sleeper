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

package sleeper.build.github.api;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;

import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;

public class GitHubApiTestHelper {
    private GitHubApiTestHelper() {
    }

    public static MappingBuilder gitHubRequest(MappingBuilder builder) {
        return builder.withHeader("Accept", equalTo("application/vnd.github+json"))
                .withHeader("Authorization", equalTo("Bearer test-bearer-token"));
    }

    public static ResponseDefinitionBuilder gitHubResponse() {
        return aResponse()
                .withHeader("Content-Type", "application/vnd.github+json");
    }

    public static RequestPatternBuilder gitHubRequest(RequestPatternBuilder builder) {
        return builder.withHeader("Accept", equalTo("application/vnd.github+json"))
                .withHeader("Authorization", equalTo("Bearer test-bearer-token"));
    }

    public static void doWithGitHubApi(WireMockRuntimeInfo runtimeInfo, Consumer<GitHubApi> consumer) {
        returnWithGitHubApi(runtimeInfo, api -> {
            consumer.accept(api);
            return null;
        });
    }

    public static <T> T returnWithGitHubApi(WireMockRuntimeInfo runtimeInfo, Function<GitHubApi, T> function) {
        try (GitHubApi api = GitHubApi.withBaseUrlAndToken(
                "http://localhost:" + runtimeInfo.getHttpPort(), "test-bearer-token")) {
            return function.apply(api);
        }
    }

}
