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
package sleeper.build.github;

import sleeper.build.testutil.TestProperties;

public class TestGitHubHead {

    private TestGitHubHead() {
    }

    public static GitHubHead example() {
        return exampleBuilder().build();
    }

    public static GitHubHead exampleFromProperties() {
        return GitHubHead.from(TestProperties.example("examples/config/github.properties"));
    }

    public static GitHubHead.Builder exampleBuilder() {
        return GitHubHead.builder().owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha");
    }
}
