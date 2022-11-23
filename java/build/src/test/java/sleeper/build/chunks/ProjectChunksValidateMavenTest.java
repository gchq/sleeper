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
package sleeper.build.chunks;

import org.junit.Test;
import sleeper.build.maven.MavenModuleStructure;
import sleeper.build.maven.TestMavenModuleStructure;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProjectChunksValidateMavenTest {

    @Test
    public void shouldValidateWhenAllCompiledModulesAreInChunks() {
        // Given
        ProjectChunks chunks = chunks(
                chunk("common", "core", "configuration"),
                chunk("ingest", "ingest"),
                chunk("bulk-import",
                        "bulk-import/bulk-import-common",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter"));
        MavenModuleStructure structure = TestMavenModuleStructure.example();

        // When / Then
        assertThatNoException().isThrownBy(() ->
                chunks.validateAllConfigured(structure));
    }

    @Test
    public void shouldFailValidationWhenOneCompiledModuleNotInAChunk() {
        // Given
        ProjectChunks chunks = chunks(
                chunk("common", "core"),
                chunk("ingest", "ingest"),
                chunk("bulk-import",
                        "bulk-import/bulk-import-common",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter"));
        MavenModuleStructure structure = TestMavenModuleStructure.example();

        // When / Then
        assertThatThrownBy(() -> chunks.validateAllConfigured(structure))
                .isInstanceOfSatisfying(NotAllMavenModulesConfiguredException.class, e ->
                        assertThat(e.getUnconfiguredModuleRefs())
                                .containsExactly("configuration"));
    }

    @Test
    public void shouldFailValidationWhenSeveralCompiledModulesNotInAChunk() {
        // Given
        ProjectChunks chunks = chunks(
                chunk("common", "core"),
                chunk("ingest", "ingest"),
                chunk("bulk-import",
                        "bulk-import/bulk-import-common",
                        "bulk-import/bulk-import-starter"));
        MavenModuleStructure structure = TestMavenModuleStructure.example();

        // When / Then
        assertThatThrownBy(() -> chunks.validateAllConfigured(structure))
                .isInstanceOfSatisfying(NotAllMavenModulesConfiguredException.class, e ->
                        assertThat(e.getUnconfiguredModuleRefs())
                                .containsExactly("configuration", "bulk-import/bulk-import-runner"));
    }

    private static ProjectChunks chunks(ProjectChunk... chunks) {
        return new ProjectChunks(Arrays.asList(chunks));
    }

    private static ProjectChunk chunk(String id, String... modules) {
        return ProjectChunk.chunk(id).name(id).workflow(id + ".yaml").modulesArray(modules).build();
    }
}
