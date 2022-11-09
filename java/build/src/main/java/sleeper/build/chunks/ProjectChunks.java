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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class ProjectChunks {

    private final List<ProjectChunk> chunks;

    public ProjectChunks(List<ProjectChunk> chunks) {
        this.chunks = Collections.unmodifiableList(Objects.requireNonNull(chunks, "chunks must not be null"));
    }

    public ProjectChunk getById(String id) {
        return stream().filter(chunk -> id.equals(chunk.getId()))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Chunk ID not found: " + id));
    }

    public Stream<ProjectChunk> stream() {
        return chunks.stream();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectChunks that = (ProjectChunks) o;
        return chunks.equals(that.chunks);
    }

    @Override
    public int hashCode() {
        return chunks.hashCode();
    }

    @Override
    public String toString() {
        return chunks.toString();
    }

    public static ProjectChunks fromYaml(Reader reader) throws IOException {
        return new ProjectChunks(listFromYaml(reader));
    }

    public static ProjectChunks fromYamlPath(String path) throws IOException {
        try (Reader reader = Files.newBufferedReader(Paths.get(path))) {
            return fromYaml(reader);
        }
    }

    private static List<ProjectChunk> listFromYaml(Reader reader) throws IOException {
        ObjectMapper mapper = new YAMLMapper();
        Map<String, Object> root = (Map<String, Object>) mapper.readValue(reader, Map.class);
        Map<String, Object> chunksMap = (Map<String, Object>) root.get("chunks");
        List<ProjectChunk> chunks = new ArrayList<>(chunksMap.size());
        for (Map.Entry<String, Object> entry : chunksMap.entrySet()) {
            String id = entry.getKey();
            Map<String, Object> config = (Map<String, Object>) entry.getValue();
            chunks.add(fromYaml(config, id));
        }
        return chunks;
    }

    private static ProjectChunk fromYaml(Map<String, Object> config, String id) {
        return ProjectChunk.chunk(id)
                .name((String) config.get("name"))
                .workflow((String) config.get("workflow"))
                .modules((List<String>) config.get("modules"))
                .build();
    }
}
