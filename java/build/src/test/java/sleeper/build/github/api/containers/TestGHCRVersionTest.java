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
package sleeper.build.github.api.containers;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static sleeper.build.github.api.TestGitHubJson.gitHubJson;

class TestGHCRVersionTest {

    @Test
    void canBuildJson() {
        assertThatJson(gitHubJson(TestGHCRVersion.version().id(123)
                .updatedAt(Instant.parse("2023-01-20T14:44:42Z"))
                .tags("latest", "test-tag")
                .build()))
                .isEqualTo("{" +
                        "\"id\":123," +
                        "\"updated_at\":\"2023-01-20T14:44:42Z\"," +
                        "\"metadata\":{" +
                        "\"package_type\":\"container\"," +
                        "\"container\":{\"tags\":[\"latest\",\"test-tag\"]}" +
                        "}}");
    }
}
