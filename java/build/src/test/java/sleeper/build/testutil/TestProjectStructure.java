/*
 * Copyright 2022-2024 Crown Copyright
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

    private static final Path BASE_PATH = basePath();

    public static ProjectStructure example() {
        return exampleBuilder().build();
    }

    public static ProjectStructure exampleWithMavenPath(String path) {
        return exampleBuilder().mavenProjectPath(BASE_PATH.resolve(path)).build();
    }

    private static ProjectStructure.Builder exampleBuilder() {
        return ProjectStructure.builder()
                .chunksYamlPath(BASE_PATH.resolve("config/chunks.yaml"))
                .mavenProjectPath(mavenPath())
                .workflowsPath(BASE_PATH.resolve("github-actions"));
    }

    public static Path mavenPath() {
        return BASE_PATH.resolve("maven");
    }

    public static CheckGitHubStatusConfig loadExampleGitHubStatusConfiguration() throws IOException {
        return CheckGitHubStatusConfig.fromGitHubAndChunks(
                BASE_PATH.resolve("config/github.properties"),
                BASE_PATH.resolve("config/chunks.yaml"));
    }

    private static Path basePath() {
        Path relativeDir = Paths.get("src/test/resources/examples");
        Path workingDir = Paths.get("").toAbsolutePath();
        if (workingDir.endsWith("build")) {
            return relativeDir;
        } else if (workingDir.endsWith("java")) {
            return Paths.get("build").resolve(relativeDir);
        } else {
            return Paths.get("java/build").resolve(relativeDir);
        }
    }
}
