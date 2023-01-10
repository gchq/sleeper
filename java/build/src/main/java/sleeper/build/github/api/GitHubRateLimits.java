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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;

import java.time.Instant;

public class GitHubRateLimits {

    private GitHubRateLimits() {
    }

    public static JsonNode get(String token) {
        return get("https://api.github.com", token);
    }

    public static JsonNode get(String baseUrl, String token) {
        Client client = ClientBuilder.newBuilder()
                .register(JacksonProvider.class)
                .build();
        Invocation request = client.target(baseUrl).path("rate_limit")
                .request("application/vnd.github+json")
                .header("Authorization", "Bearer " + token)
                .buildGet();
        return request.invoke().readEntity(JsonNode.class);
    }

    public static int remainingLimit(JsonNode response) {
        return response.get("resources").get("core").get("remaining").asInt();
    }

    public static Instant resetTime(JsonNode response) {
        return Instant.ofEpochSecond(response.get("resources").get("core").get("reset").asLong());
    }
}
