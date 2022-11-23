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
package sleeper.build.github.actions;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.build.testutil.TestResources.exampleReader;

public class GitHubActionsChunkWorkflowYamlTest {

    @Test
    public void shouldReadGitHubActionsWorkflowForBulkImportChunk() throws IOException {
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.read(
                exampleReader("examples/github-actions/chunk-bulk-import.yaml"));

        assertThat(workflow).isEqualTo(
                GitHubActionsChunkWorkflow.builder()
                        .chunkId("bulk-import")
                        .name("Build Bulk Import Modules")
                        .onPushPathsArray(
                                "github-actions/chunk-bulk-import.yaml",
                                "github-actions/chunk.yaml",
                                "config/chunks.yaml",
                                "maven/pom.xml",
                                "maven/bulk-import/pom.xml",
                                "maven/bulk-import/bulk-import-runner/**",
                                "maven/bulk-import/bulk-import-starter/**",
                                "maven/bulk-import/bulk-import-common/**",
                                "maven/ingest/**",
                                "maven/configuration/**",
                                "maven/core/**")
                        .build());
    }

    @Test
    public void shouldReadGitHubActionsWorkflowForCommonChunk() throws IOException {
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.read(
                exampleReader("examples/github-actions/chunk-common.yaml"));

        assertThat(workflow).isEqualTo(
                GitHubActionsChunkWorkflow.builder()
                        .chunkId("common")
                        .name("Build Common Modules")
                        .onPushPathsArray(
                                "github-actions/chunk-common.yaml",
                                "github-actions/chunk.yaml",
                                "config/chunks.yaml",
                                "maven/pom.xml",
                                "maven/configuration/**",
                                "maven/core/**")
                        .build());
    }

    @Test
    public void shouldFailWhenMoreThanOneJobDeclared() {
        // Given
        Map<String, GitHubActionsChunkWorkflowYaml.Job> jobs = new HashMap<>();
        jobs.put("some-job", job("some-workflow", "some-chunk"));
        jobs.put("other-job", job("other-workflow", "other-chunk"));

        // When / Then
        assertThatThrownBy(() -> workflowWithJobs(jobs))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static GitHubActionsChunkWorkflowYaml workflowWithJobs(
            Map<String, GitHubActionsChunkWorkflowYaml.Job> jobs) {
        return new GitHubActionsChunkWorkflowYaml("name", onPushPaths(), jobs);
    }

    private static GitHubActionsChunkWorkflowYaml.Job job(String workflow, String chunkId) {
        return new GitHubActionsChunkWorkflowYaml.Job(workflow,
                new GitHubActionsChunkWorkflowYaml.WorkflowInputs(chunkId));
    }

    private static GitHubActionsChunkWorkflowYaml.On onPushPaths(String... paths) {
        return new GitHubActionsChunkWorkflowYaml.On(
                new GitHubActionsChunkWorkflowYaml.Push(Arrays.asList(paths)));
    }


}
