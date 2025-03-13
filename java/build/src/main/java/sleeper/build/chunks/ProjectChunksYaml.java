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
package sleeper.build.chunks;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProjectChunksYaml {

    private final Map<String, YamlChunk> chunks;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ProjectChunksYaml(@JsonProperty("chunks") Map<String, YamlChunk> chunks) {
        this.chunks = chunks;
    }

    public static ProjectChunks readPath(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return read(reader);
        }
    }

    public static ProjectChunks read(Reader reader) throws IOException {
        ObjectMapper mapper = new YAMLMapper();
        ProjectChunksYaml chunksYaml = mapper.readValue(reader, ProjectChunksYaml.class);
        return new ProjectChunks(chunksYaml.getChunks());
    }

    private List<ProjectChunk> getChunks() {
        return chunks.entrySet().stream()
                .map(entry -> entry.getValue().build(entry.getKey()))
                .collect(Collectors.toList());
    }

    public static class YamlChunk {
        private final String name;
        private final String workflow;
        private final List<String> modules;

        @JsonAnySetter
        private final Map<String, JsonNode> other = new LinkedHashMap<>();

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public YamlChunk(
                @JsonProperty("name") String name,
                @JsonProperty("workflow") String workflow,
                @JsonProperty("modules") List<String> modules) {
            this.name = name;
            this.workflow = workflow;
            this.modules = modules;
        }

        public ProjectChunk build(String id) {
            return ProjectChunk.chunk(id).name(name).workflow(workflow).modules(modules)
                    .workflowOnlyProperties(getWorkflowOnlyProperties()).build();
        }

        private Map<String, String> getWorkflowOnlyProperties() {
            Map<String, String> workflowOnlyProperties = new LinkedHashMap<>();
            other.forEach((key, value) -> workflowOnlyProperties.put(key, readWorkflowOnlyValue(value)));
            return workflowOnlyProperties;
        }

        private static String readWorkflowOnlyValue(JsonNode value) {
            if (value.isTextual()) {
                return value.textValue();
            } else {
                return value.toString();
            }
        }
    }
}
