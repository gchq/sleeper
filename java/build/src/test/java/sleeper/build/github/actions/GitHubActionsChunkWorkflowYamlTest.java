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
package sleeper.build.github.actions;

import org.junit.jupiter.api.Test;

import sleeper.build.chunks.TestChunks;
import sleeper.build.testutil.TestProjectStructure;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.github.actions.TestGitHubActionsChunkWorkflows.workflow;
import static sleeper.build.testutil.TestResources.exampleReader;

public class GitHubActionsChunkWorkflowYamlTest {

    @Test
    public void shouldReadBulkImportExampleWorkflow() throws Exception {
        // Given
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.read(
                exampleReader("examples/github-actions/chunk-bulk-import.yaml"));

        // When / Then
        assertThat(workflow).isEqualTo(TestGitHubActionsChunkWorkflows.bulkImport());
    }

    @Test
    public void shouldReadCommonExampleWorkflow() throws Exception {
        // Given
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.read(
                exampleReader("examples/github-actions/chunk-common.yaml"));

        // When / Then
        assertThat(workflow).isEqualTo(TestGitHubActionsChunkWorkflows.common());
    }

    @Test
    public void shouldReadCommonExampleWorkflowFromPath() throws Exception {
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.readFromPath(
                TestProjectStructure.example().workflowPath(TestChunks.common()));

        assertThat(workflow).isEqualTo(TestGitHubActionsChunkWorkflows.common());
    }

    @Test
    public void shouldIgnoreExtraJob() throws Exception {
        // Given
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.read(
                exampleReader("examples/github-actions/corner-cases/chunk-extra-job.yaml"));

        // When / Then
        assertThat(workflow).isEqualTo(workflow("some-chunk", "github-actions/chunk-extra-job.yaml"));
    }

    @Test
    public void shouldIgnoreExtraJobFirst() throws Exception {
        // Given
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.read(
                exampleReader("examples/github-actions/corner-cases/chunk-extra-job-first.yaml"));

        // When / Then
        assertThat(workflow).isEqualTo(workflow("some-chunk", "github-actions/chunk-extra-job.yaml"));
    }

    @Test
    public void shouldIgnoreExtraInput() throws Exception {
        // Given
        GitHubActionsChunkWorkflow workflow = GitHubActionsChunkWorkflowYaml.read(
                exampleReader("examples/github-actions/corner-cases/chunk-extra-input.yaml"));

        // When / Then
        assertThat(workflow).isEqualTo(workflow("some-chunk", "github-actions/chunk-extra-input.yaml"));
    }

}
