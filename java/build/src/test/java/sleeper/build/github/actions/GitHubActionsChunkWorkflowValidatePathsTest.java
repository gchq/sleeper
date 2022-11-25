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
import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectStructure;
import sleeper.build.chunks.TestChunks;
import sleeper.build.chunks.TestProjectStructure;
import sleeper.build.maven.InternalDependencyIndex;
import sleeper.build.maven.TestMavenModuleStructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GitHubActionsChunkWorkflowValidatePathsTest {

    @Test
    public void shouldValidateCommonExample() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.common();
        ProjectChunk chunk = TestChunks.common();
        ProjectStructure project = TestProjectStructure.example();
        InternalDependencyIndex maven = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThat(chunk.getExpectedPathsToTriggerBuild(project, maven, workflow)).containsExactly(
                "github-actions/chunk-common.yaml",
                "github-actions/chunk.yaml",
                "config/chunks.yaml",
                "maven/pom.xml",
                "maven/core/**",
                "maven/configuration/**");
        assertThatCode(() -> workflow.validate(project, chunk, maven))
                .doesNotThrowAnyException();
    }

    @Test
    public void shouldValidateBulkImportExample() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.bulkImport();
        ProjectChunk chunk = TestChunks.bulkImport();
        ProjectStructure project = TestProjectStructure.example();
        InternalDependencyIndex maven = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThat(chunk.getExpectedPathsToTriggerBuild(project, maven, workflow)).containsExactly(
                "github-actions/chunk-bulk-import.yaml",
                "github-actions/chunk.yaml",
                "config/chunks.yaml",
                "maven/pom.xml",
                "maven/bulk-import/pom.xml",
                "maven/bulk-import/bulk-import-common/**",
                "maven/configuration/**",
                "maven/core/**",
                "maven/bulk-import/bulk-import-starter/**",
                "maven/bulk-import/bulk-import-runner/**",
                "maven/ingest/**");
        assertThatCode(() -> workflow.validate(project, chunk, maven))
                .doesNotThrowAnyException();
    }

    @Test
    public void shouldFailWhenDependencyMissingFromOnPushPaths() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.workflow("common",
                "github-actions/chunk-common.yaml",
                "github-actions/chunk.yaml",
                "config/chunks.yaml",
                "maven/pom.xml",
                "maven/core/**");
        ProjectChunk chunk = TestChunks.common();
        ProjectStructure project = TestProjectStructure.example();
        InternalDependencyIndex maven = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThatThrownBy(() -> workflow.validate(project, chunk, maven))
                .isInstanceOfSatisfying(NotAllDependenciesDeclaredException.class, e -> {
                    assertThat(e.getChunkId()).isEqualTo("common");
                    assertThat(e.getUnconfiguredModuleRefs()).containsExactly("configuration");
                });
    }

    @Test
    public void shouldFailWhenMultipleDependenciesMissingFromOnPushPaths() {
        // Given
        GitHubActionsChunkWorkflow workflow = TestGitHubActionsChunkWorkflows.workflow("common",
                "github-actions/chunk-common.yaml",
                "github-actions/chunk.yaml",
                "config/chunks.yaml",
                "maven/pom.xml");
        ProjectChunk chunk = TestChunks.common();
        ProjectStructure project = TestProjectStructure.example();
        InternalDependencyIndex maven = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThatThrownBy(() -> workflow.validate(project, chunk, maven))
                .isInstanceOfSatisfying(NotAllDependenciesDeclaredException.class, e -> {
                    assertThat(e.getChunkId()).isEqualTo("common");
                    assertThat(e.getUnconfiguredModuleRefs()).containsExactly("core", "configuration");
                });
    }
}
