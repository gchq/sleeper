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
package sleeper.build.ratelimit;

import com.fasterxml.jackson.databind.JsonNode;

import sleeper.build.github.api.GitHubRateLimits;

public class GetRemainingRateLimit {

    private GetRemainingRateLimit() {
    }

    public static void main(String[] args) {
        JsonNode response = GitHubRateLimits.get(args[0]);
        int remaining = GitHubRateLimits.remainingLimit(response);
        System.err.println("Core limit remaining: " + remaining);
        System.err.println("Core limit resets at: " + GitHubRateLimits.resetTime(response));
        System.err.println(response.toPrettyString());
        System.out.println(remaining);
    }

}
