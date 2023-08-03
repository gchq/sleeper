/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.build.maven.InternalModuleIndex;
import sleeper.build.maven.MavenModuleAndPath;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static sleeper.build.util.ValidationUtils.ignoreEmpty;

public class ProjectChunk {

    private final String id;
    private final String name;
    private final String workflow;
    private final List<String> modules;

    // These are properties that will be exposed to the workflow by GetChunkConfig, but should otherwise be ignored.
    private final Map<String, String> workflowOnlyProperties;

    private ProjectChunk(Builder builder) {
        id = Objects.requireNonNull(ignoreEmpty(builder.id), "id must not be null");
        name = Objects.requireNonNull(ignoreEmpty(builder.name), "name must not be null");
        workflow = Objects.requireNonNull(ignoreEmpty(builder.workflow), "workflow must not be null");
        modules = Collections.unmodifiableList(Objects.requireNonNull(builder.modules, "modules must not be null"));
        workflowOnlyProperties = Collections.unmodifiableMap(Objects.requireNonNull(builder.workflowOnlyProperties, "workflowOnlyProperties must not be null"));
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getWorkflow() {
        return workflow;
    }

    public List<String> getModules() {
        return modules;
    }

    public String getMavenProjectList() {
        return String.join(",", getModules());
    }

    public Map<String, String> getWorkflowOutputs() {
        return workflowOnlyProperties;
    }

    public Stream<MavenModuleAndPath> dependencies(InternalModuleIndex index) {
        return index.dependenciesForModules(modules);
    }

    public static Builder chunk(String id) {
        return new Builder().id(id);
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
        return id.equals(that.id) && name.equals(that.name)
                && workflow.equals(that.workflow) && modules.equals(that.modules)
                && workflowOnlyProperties.equals(that.workflowOnlyProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, workflow, modules, workflowOnlyProperties);
    }

    @Override
    public String toString() {
        return "ProjectChunk{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", workflow='" + workflow + '\'' +
                ", modules=" + modules +
                ", workflowOnlyProperties=" + workflowOnlyProperties +
                '}';
    }

    public static final class Builder {
        private String id;
        private String name;
        private String workflow;
        private List<String> modules = Collections.emptyList();
        private Map<String, String> workflowOnlyProperties = Collections.emptyMap();

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

        public Builder workflowOnlyProperties(Map<String, String> workflowOnlyProperties) {
            this.workflowOnlyProperties = workflowOnlyProperties;
            return this;
        }

        public ProjectChunk build() {
            return new ProjectChunk(this);
        }
    }
}
