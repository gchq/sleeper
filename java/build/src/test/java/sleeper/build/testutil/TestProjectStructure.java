/*
 * Copyright 2022 Crown Copyright
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
package sleeper.build.testutil;

import sleeper.build.chunks.ProjectStructure;
import sleeper.build.status.CheckGitHubStatusConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestProjectStructure {

    private TestProjectStructure() {
    }

    private static final Path BASE_PATH = Paths.get("src/test/resources/examples");

    public static ProjectStructure example() {
        return ProjectStructure.builder()
                .chunksYamlPath(BASE_PATH.resolve("config/chunks.yaml"))
                .mavenProjectPath(BASE_PATH.resolve("maven"))
                .workflowsPath(BASE_PATH.resolve("github-actions"))
                .build();
    }

    public static CheckGitHubStatusConfig loadExampleGitHubStatusConfiguration() throws IOException {
        return CheckGitHubStatusConfig.fromGitHubAndChunks(
                BASE_PATH.resolve("config/github.properties"),
                BASE_PATH.resolve("config/chunks.yaml"));
    }
}
