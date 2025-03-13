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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

public class GitHubApi implements AutoCloseable {
    private final Client client = ClientBuilder.newBuilder()
            .register(JacksonProvider.class)
            .build();
    private final WebTarget baseTarget;
    private final String token;

    private GitHubApi(String baseUrl, String token) {
        baseTarget = client.target(baseUrl);
        this.token = token;
    }

    public WebTarget path(String path) {
        return baseTarget.path(path);
    }

    public Invocation.Builder request(WebTarget target) {
        return target.request("application/vnd.github+json")
                .header("Authorization", "Bearer " + token);
    }

    public static GitHubApi withToken(String token) {
        return withBaseUrlAndToken("https://api.github.com", token);
    }

    public static GitHubApi withBaseUrlAndToken(String baseUrl, String token) {
        return new GitHubApi(baseUrl, token);
    }

    @Override
    public void close() {
        client.close();
    }
}
