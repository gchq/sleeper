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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProjectChunksTest {

    private static final ProjectChunks CHUNKS = TestChunks.example();

    @Test
    public void shouldReadExample() {
        assertThat(TestChunks.example("examples/config/chunks.yaml"))
                .isEqualTo(CHUNKS);
    }

    @Test
    public void shouldGetChunkConfigById() {
        ProjectChunk chunk = CHUNKS.getById("bulk-import");
        assertThat(chunk.getName()).isEqualTo("Bulk Import");
        assertThat(chunk.getWorkflow()).isEqualTo("chunk-bulk-import.yaml");
        assertThat(chunk.getMavenProjectList()).isEqualTo(
                "bulk-import/bulk-import-core,bulk-import/bulk-import-starter,bulk-import/bulk-import-runner");
    }

    @Test
    public void shouldRefuseChunkNotFound() {
        assertThatThrownBy(() -> CHUNKS.getById("not-a-chunk"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
