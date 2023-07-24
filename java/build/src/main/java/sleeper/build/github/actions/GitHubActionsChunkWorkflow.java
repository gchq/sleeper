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
package sleeper.build.github.actions;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectStructure;
import sleeper.build.maven.InternalDependencyIndex;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GitHubActionsChunkWorkflow {

    private final String chunkId;
    private final String name;
    private final Path usesWorkflowPath;
    private final List<String> onTriggerPaths;

    private GitHubActionsChunkWorkflow(Builder builder) {
        chunkId = Objects.requireNonNull(builder.chunkId, "chunkId must not be null");
        name = Objects.requireNonNull(builder.name, "name must not be null");
        usesWorkflowPath = Objects.requireNonNull(builder.usesWorkflowPath, "usesWorkflowPath must not be null");
        onTriggerPaths = Objects.requireNonNull(builder.onTriggerPaths, "onTriggerPaths must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public Path getUsesWorkflowPath() {
        return usesWorkflowPath;
    }

    public WorkflowTriggerPathsDiff getTriggerPathsDiffFromExpected(
            ProjectStructure project, ProjectChunk chunk, InternalDependencyIndex index) {
        return WorkflowTriggerPathsDiff.fromExpectedAndActual(project,
                ExpectedWorkflowTriggerPaths.from(project, index, chunk, this),
                onTriggerPaths);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GitHubActionsChunkWorkflow that = (GitHubActionsChunkWorkflow) o;
        return chunkId.equals(that.chunkId) && name.equals(that.name) && usesWorkflowPath.equals(that.usesWorkflowPath) && onTriggerPaths.equals(that.onTriggerPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkId, name, usesWorkflowPath, onTriggerPaths);
    }

    @Override
    public String toString() {
        return "GitHubActionsChunkWorkflow{" +
                "chunkId='" + chunkId + '\'' +
                ", name='" + name + '\'' +
                ", usesWorkflowPath=" + usesWorkflowPath +
                ", onPushPaths=" + onTriggerPaths +
                '}';
    }

    public static final class Builder {
        private List<String> onTriggerPaths;
        private String chunkId;
        private String name;
        private Path usesWorkflowPath;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder chunkId(String chunkId) {
            this.chunkId = chunkId;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder usesWorkflowPath(Path usesWorkflowPath) {
            this.usesWorkflowPath = usesWorkflowPath;
            return this;
        }

        public Builder onTriggerPaths(List<String> onTriggerPaths) {
            this.onTriggerPaths = onTriggerPaths;
            return this;
        }

        public Builder onTriggerPathsArray(String... onTriggerPaths) {
            return onTriggerPaths(Arrays.asList(onTriggerPaths));
        }

        public GitHubActionsChunkWorkflow build() {
            return new GitHubActionsChunkWorkflow(this);
        }
    }
}
