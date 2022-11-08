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

import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static sleeper.build.util.ValidationUtils.ignoreEmpty;

public class ProjectChunk {

    private final String id;
    private final String name;
    private final String workflow;
    private final List<String> modules;

    private ProjectChunk(Builder builder) {
        id = Objects.requireNonNull(ignoreEmpty(builder.id), "id must not be null");
        name = Objects.requireNonNull(ignoreEmpty(builder.name), "name must not be null");
        workflow = Objects.requireNonNull(ignoreEmpty(builder.workflow), "workflow must not be null");
        modules = Objects.requireNonNull(builder.modules, "modules must not be null");
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public static Builder chunk(String id) {
        return new Builder().id(id);
    }

    public static List<ProjectChunk> listFrom(Properties properties) {
        String[] chunkIds = properties.getProperty("chunks").split(",");
        List<ProjectChunk> chunks = new ArrayList<>(chunkIds.length);
        for (String id : chunkIds) {
            chunks.add(from(properties, id));
        }
        return chunks;
    }

    public static List<ProjectChunk> listFromYaml(Reader reader) {
        LoadSettings settings = LoadSettings.builder().build();
        Load load = new Load(settings);
        Map<String, Object> root = (Map<String, Object>) load.loadFromReader(reader);
        Map<String, Object> chunksMap = (Map<String, Object>) root.get("chunks");
        List<ProjectChunk> chunks = new ArrayList<>(chunksMap.size());
        for (Map.Entry<String, Object> entry : chunksMap.entrySet()) {
            String id = entry.getKey();
            Map<String, Object> config = (Map<String, Object>) entry.getValue();
            chunks.add(fromYaml(config, id));
        }
        return chunks;
    }

    private static ProjectChunk from(Properties properties, String id) {
        return chunk(id)
                .name(properties.getProperty("chunk." + id + ".name"))
                .workflow(properties.getProperty("chunk." + id + ".workflow"))
                .modulesArray()
                .build();
    }

    private static ProjectChunk fromYaml(Map<String, Object> config, String id) {
        return chunk(id)
                .name((String) config.get("name"))
                .workflow((String) config.get("workflow"))
                .modules((List<String>) config.get("modules"))
                .build();
    }

    public String getWorkflow() {
        return workflow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectChunk that = (ProjectChunk) o;
        return id.equals(that.id) && name.equals(that.name) && workflow.equals(that.workflow) && modules.equals(that.modules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, workflow, modules);
    }

    @Override
    public String toString() {
        return "ProjectChunk{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", workflow='" + workflow + '\'' +
                ", modules=" + modules +
                '}';
    }

    public static final class Builder {
        private String id;
        private String name;
        private String workflow;
        private List<String> modules;

        private Builder() {
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder workflow(String workflow) {
            this.workflow = workflow;
            return this;
        }

        public Builder modules(List<String> modules) {
            this.modules = modules;
            return this;
        }

        public Builder modulesArray(String... modules) {
            return modules(Arrays.asList(modules));
        }

        public ProjectChunk build() {
            return new ProjectChunk(this);
        }
    }
}
