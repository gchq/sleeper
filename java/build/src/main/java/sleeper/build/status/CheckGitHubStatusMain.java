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
package sleeper.build.status;

import sleeper.build.chunks.ProjectConfiguration;
import sleeper.build.chunks.ProjectStructure;
import sleeper.build.github.api.GitHubWorkflowRunsImpl;

import java.io.IOException;
import java.nio.file.Paths;

public class CheckGitHubStatusMain {

    private CheckGitHubStatusMain() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: <github.properties path> <chunks.yaml path> <Maven project base path>");
            System.exit(1);
            return;
        }
        ProjectStructure structure = ProjectStructure.builder()
                .gitHubPropertiesPath(Paths.get(args[0]))
                .chunksYamlPath(Paths.get(args[1]))
                .mavenProjectPath(Paths.get(args[2]))
                .build();
        ProjectConfiguration configuration = structure.loadProjectConfiguration();
        try {
            configuration.validate(structure, System.err);
        } catch (IllegalStateException e) {
            System.exit(1);
            return;
        }
        try (GitHubWorkflowRunsImpl gitHub = configuration.gitHubWorkflowRuns()) {
            ChunkStatuses status = configuration.checkStatus(gitHub);
            status.report(System.out);
            if (status.isFailCheck()) {
                System.exit(1);
            }
        }
    }
}
