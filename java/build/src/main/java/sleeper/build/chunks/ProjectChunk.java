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

import sleeper.build.github.actions.GitHubActionsChunkWorkflow;
import sleeper.build.maven.InternalDependencyIndex;
import sleeper.build.maven.MavenModuleAndPath;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        modules = Collections.unmodifiableList(Objects.requireNonNull(builder.modules, "modules must not be null"));
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

    public Stream<MavenModuleAndPath> dependencies(InternalDependencyIndex index) {
        return index.dependenciesForModules(modules);
    }

    public List<String> getExpectedPathsToTriggerBuild(
            ProjectStructure project, InternalDependencyIndex maven, GitHubActionsChunkWorkflow actualWorkflow) {
        return Stream.concat(Stream.concat(
                                Stream.of(
                                        project.workflowPathInRepository(this).toString(),
                                        actualWorkflow.getUsesWorkflowPath().normalize().toString(),
                                        project.getChunksYamlRelative().toString()),
                                maven.ancestorsForModules(modules).map(module ->
                                        module.pomPathInRepository(project).toString())),
                        maven.dependenciesForModules(modules).map(module ->
                                module.pathInRepository(project).toString() + "/**"))
                .collect(Collectors.toList());
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
        private List<String> modules = Collections.emptyList();

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
