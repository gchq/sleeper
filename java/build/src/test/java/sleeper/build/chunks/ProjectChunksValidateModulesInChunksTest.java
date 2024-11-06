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
package sleeper.build.chunks;

import org.junit.jupiter.api.Test;

import sleeper.build.maven.MavenModuleStructure;
import sleeper.build.maven.TestMavenModuleStructure;
import sleeper.build.testutil.TestProjectStructure;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.build.chunks.TestChunks.chunk;
import static sleeper.build.chunks.TestChunks.chunks;

public class ProjectChunksValidateModulesInChunksTest {

    private final OutputStream outputStream = new ByteArrayOutputStream();
    private final ProjectStructure project = TestProjectStructure.example();
    private final MavenModuleStructure maven = TestMavenModuleStructure.example();

    @Test
    public void shouldValidateWhenAllJavaModulesAreInChunks() {
        // Given
        ProjectChunks chunks = TestChunks.example();

        // When
        validateAllConfigured(chunks);

        // Then
        assertThat(outputStream.toString()).isEmpty();
    }

    @Test
    public void shouldFailValidationWhenOneJavaModuleNotInAChunk() {
        // Given
        ProjectChunks chunks = chunks(
                chunk("common", "core"),
                chunk("ingest", "ingest"),
                chunk("bulk-import",
                        "bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter"));

        // When / Then
        assertThatThrownBy(() -> validateAllConfigured(chunks))
                .isInstanceOf(ProjectChunksValidationException.class);
        assertThat(outputStream.toString()).contains(
                "Maven modules not configured in any chunk: configuration");
    }

    @Test
    public void shouldFailValidationWhenSeveralJavaModulesNotInAChunk() {
        // Given
        ProjectChunks chunks = chunks(
                chunk("common", "core"),
                chunk("ingest", "ingest"),
                chunk("bulk-import",
                        "bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-starter"));

        // When / Then
        assertThatThrownBy(() -> validateAllConfigured(chunks))
                .isInstanceOf(ProjectChunksValidationException.class);
        assertThat(outputStream.toString()).contains(
                "Maven modules not configured in any chunk: configuration, bulk-import/bulk-import-runner");
    }

    @Test
    public void shouldFailValidationWhenOneModuleInAChunkIsNotAJavaModule() {
        // Given
        ProjectChunks chunks = chunks(
                chunk("common", "core", "configuration"),
                chunk("ingest", "ingest"),
                chunk("bulk-import",
                        "bulk-import",
                        "bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter"));

        // When / Then
        assertThatThrownBy(() -> validateAllConfigured(chunks))
                .isInstanceOf(ProjectChunksValidationException.class);
        assertThat(outputStream.toString()).contains(
                "Maven modules with no source code found in a chunk: bulk-import");
    }

    private void validateAllConfigured(ProjectChunks chunks) {
        chunks.validateAllConfigured(project, maven, printStream(outputStream));
    }

    private static PrintStream printStream(OutputStream outputStream) {
        try {
            return new PrintStream(outputStream, false, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
