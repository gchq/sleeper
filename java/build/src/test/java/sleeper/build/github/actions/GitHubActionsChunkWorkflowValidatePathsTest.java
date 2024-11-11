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

import org.junit.jupiter.api.Test;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectStructure;
import sleeper.build.chunks.TestChunks;
import sleeper.build.maven.InternalModuleIndex;
import sleeper.build.maven.TestMavenModuleStructure;
import sleeper.build.testutil.TestProjectStructure;

import static org.assertj.core.api.Assertions.assertThat;

public class GitHubActionsChunkWorkflowValidatePathsTest {

    @Test
    public void shouldValidateCommonExample() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.common();
        ProjectChunk chunk = TestChunks.common();
        ProjectStructure project = TestProjectStructure.example();
        InternalModuleIndex maven = TestMavenModuleStructure.example().indexInternalModules();

        // When
        WorkflowTriggerPathsDiff diff = workflow.getTriggerPathsDiffFromExpected(project, chunk, maven);

        // When / Then
        assertThat(diff.getExpected()).containsExactly(
                "github-actions/chunk-common.yaml",
                "github-actions/chunk.yaml",
                "config/chunks.yaml",
                "maven/pom.xml",
                "maven/core/**",
                "maven/configuration/**");
        assertThat(diff.getMissingEntries()).isEmpty();
        assertThat(diff.getExtraEntries()).isEmpty();
    }

    @Test
    public void shouldValidateBulkImportExample() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.bulkImport();
        ProjectChunk chunk = TestChunks.bulkImport();
        ProjectStructure project = TestProjectStructure.example();
        InternalModuleIndex maven = TestMavenModuleStructure.example().indexInternalModules();

        // When
        WorkflowTriggerPathsDiff diff = workflow.getTriggerPathsDiffFromExpected(project, chunk, maven);

        // Then
        assertThat(diff.getExpected()).containsExactly(
                "github-actions/chunk-bulk-import.yaml",
                "github-actions/chunk.yaml",
                "config/chunks.yaml",
                "maven/pom.xml",
                "maven/bulk-import/pom.xml",
                "maven/bulk-import/bulk-import-core/**",
                "maven/bulk-import/bulk-import-starter/**",
                "maven/bulk-import/bulk-import-runner/**",
                "maven/ingest/**",
                "maven/configuration/**",
                "maven/core/**");
        assertThat(diff.getMissingEntries()).isEmpty();
        assertThat(diff.getExtraEntries()).isEmpty();
    }

    @Test
    public void shouldFindDependencyMissingFromOnPushPaths() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.workflow("common",
                "github-actions/chunk-common.yaml",
                "github-actions/chunk.yaml",
                "config/chunks.yaml",
                "maven/pom.xml",
                "maven/core/**");
        ProjectChunk chunk = TestChunks.common();
        ProjectStructure project = TestProjectStructure.example();
        InternalModuleIndex maven = TestMavenModuleStructure.example().indexInternalModules();

        // When
        WorkflowTriggerPathsDiff diff = workflow.getTriggerPathsDiffFromExpected(project, chunk, maven);

        // Then
        assertThat(diff.getMissingEntries()).containsExactly("maven/configuration/**");
    }

    @Test
    public void shouldFindMultipleDependenciesMissingFromOnPushPaths() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.workflow("common",
                "github-actions/chunk-common.yaml",
                "config/chunks.yaml",
                "maven/pom.xml",
                "maven/core/**");
        ProjectChunk chunk = TestChunks.common();
        ProjectStructure project = TestProjectStructure.example();
        InternalModuleIndex maven = TestMavenModuleStructure.example().indexInternalModules();

        // When
        WorkflowTriggerPathsDiff diff = workflow.getTriggerPathsDiffFromExpected(project, chunk, maven);

        // Then
        assertThat(diff.getMissingEntries()).containsExactly(
                "github-actions/chunk.yaml",
                "maven/configuration/**");
    }
}
