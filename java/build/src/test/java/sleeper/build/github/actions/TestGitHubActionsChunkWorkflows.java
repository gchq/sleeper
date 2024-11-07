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
package sleeper.build.github.actions;

import java.nio.file.Paths;

public class TestGitHubActionsChunkWorkflows {

    private TestGitHubActionsChunkWorkflows() {
    }

    public static GitHubActionsChunkWorkflow bulkImport() {
        return GitHubActionsChunkWorkflow.builder()
                .chunkId("bulk-import")
                .name("Build Bulk Import Modules")
                .usesWorkflowPath(Paths.get("./github-actions/chunk.yaml"))
                .onTriggerPathsArray(
                        "github-actions/chunk-bulk-import.yaml",
                        "github-actions/chunk.yaml",
                        "config/chunks.yaml",
                        "maven/pom.xml",
                        "maven/bulk-import/pom.xml",
                        "maven/bulk-import/bulk-import-runner/**",
                        "maven/bulk-import/bulk-import-starter/**",
                        "maven/bulk-import/bulk-import-core/**",
                        "maven/ingest/**",
                        "maven/configuration/**",
                        "maven/core/**")
                .build();
    }

    public static GitHubActionsChunkWorkflow common() {
        return GitHubActionsChunkWorkflow.builder()
                .chunkId("common")
                .name("Build Common Modules")
                .usesWorkflowPath(Paths.get("./github-actions/chunk.yaml"))
                .onTriggerPathsArray(
                        "github-actions/chunk-common.yaml",
                        "github-actions/chunk.yaml",
                        "config/chunks.yaml",
                        "maven/pom.xml",
                        "maven/configuration/**",
                        "maven/core/**")
                .build();
    }

    public static GitHubActionsChunkWorkflow workflow(String chunkId, String... paths) {
        return GitHubActionsChunkWorkflow.builder()
                .chunkId(chunkId).name(chunkId)
                .usesWorkflowPath(Paths.get("./github-actions/chunk.yaml"))
                .onTriggerPathsArray(paths)
                .build();
    }

}
