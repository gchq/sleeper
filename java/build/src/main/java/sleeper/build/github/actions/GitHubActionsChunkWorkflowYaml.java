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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubActionsChunkWorkflowYaml {

    private final List<String> onPushPaths;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GitHubActionsChunkWorkflowYaml(@JsonProperty("on") On on) {
        this.onPushPaths = on.push.paths;
    }

    public static GitHubActionsChunkWorkflow read(Reader reader) throws IOException {
        ObjectMapper mapper = new YAMLMapper();
        GitHubActionsChunkWorkflowYaml chunksYaml = mapper.readValue(reader, GitHubActionsChunkWorkflowYaml.class);
        return GitHubActionsChunkWorkflow.builder()
                .chunkId("bulk-import")
                .name("Build Bulk Import Modules")
                .onPushPaths(chunksYaml.onPushPaths)
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
}
