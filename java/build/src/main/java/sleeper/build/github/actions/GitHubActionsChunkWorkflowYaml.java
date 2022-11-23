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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

public class GitHubActionsChunkWorkflowYaml {

    private final String chunkId;
    private final String name;
    private final List<String> onPushPaths;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GitHubActionsChunkWorkflowYaml(
            @JsonProperty("name") String name,
            @JsonProperty("on") On on,
            @JsonProperty("jobs") Map<String, Job> jobs) {
        if (jobs.size() != 1) {
            throw new IllegalArgumentException("Expected only one job declared, found " + jobs.size());
        }
        Job job = jobs.values().stream().findFirst().orElseThrow(IllegalStateException::new);
        this.chunkId = job.with.chunkId;
        this.name = name;
        this.onPushPaths = on.push.paths;
    }

    public static GitHubActionsChunkWorkflow read(Reader reader) throws IOException {
        ObjectMapper mapper = new YAMLMapper();
        GitHubActionsChunkWorkflowYaml chunksYaml = mapper.readValue(reader, GitHubActionsChunkWorkflowYaml.class);
        return chunksYaml.toWorkflow();
    }

    public GitHubActionsChunkWorkflow toWorkflow() {
        return GitHubActionsChunkWorkflow.builder()
                .chunkId(chunkId)
                .name(name)
                .onPushPaths(onPushPaths)
                .build();
    }

    public static class On {
        private final Push push;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public On(@JsonProperty("push") Push push) {
            this.push = push;
        }
    }

    public static class Push {
        private final List<String> paths;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Push(@JsonProperty("paths") List<String> paths) {
            this.paths = paths;
        }
    }

    public static class Job {
        private final String uses;
        private final WorkflowInputs with;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Job(
                @JsonProperty("uses") String uses,
                @JsonProperty("with") WorkflowInputs with) {
            this.uses = uses;
            this.with = with;
        }
    }

    public static class WorkflowInputs {
        private final String chunkId;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public WorkflowInputs(@JsonProperty("chunkId") String chunkId) {
            this.chunkId = chunkId;
        }
    }
}
