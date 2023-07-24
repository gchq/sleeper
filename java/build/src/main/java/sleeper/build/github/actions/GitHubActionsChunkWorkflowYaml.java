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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class GitHubActionsChunkWorkflowYaml {

    private final String name;
    private final List<String> onTriggerPaths;
    private final Map<String, Object> jobs;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GitHubActionsChunkWorkflowYaml(
            @JsonProperty("name") String name,
            @JsonProperty("on") On on,
            @JsonProperty("jobs") Map<String, Object> jobs) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.onTriggerPaths = Objects.requireNonNull(on, "on must not be null").trigger.paths;
        this.jobs = Objects.requireNonNull(jobs, "jobs must not be null");
    }

    public static GitHubActionsChunkWorkflow read(Reader reader) throws IOException {
        ObjectMapper mapper = new YAMLMapper();
        GitHubActionsChunkWorkflowYaml chunksYaml = mapper.readValue(reader, GitHubActionsChunkWorkflowYaml.class);
        return chunksYaml.toWorkflow(mapper);
    }

    public static GitHubActionsChunkWorkflow readFromPath(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return read(reader);
        }
    }

    private GitHubActionsChunkWorkflow toWorkflow(ObjectMapper mapper) {
        Job job = chunkJob(mapper);
        return GitHubActionsChunkWorkflow.builder()
                .chunkId(job.with.chunkId)
                .name(name)
                .usesWorkflowPath(Paths.get(job.uses))
                .onTriggerPaths(onTriggerPaths)
                .build();
    }

    private Job chunkJob(ObjectMapper mapper) {
        return jobs.values().stream()
                .flatMap(job -> readJob(job, mapper))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("No chunk job found"));
    }

    private Stream<Job> readJob(Object job, ObjectMapper mapper) {
        try {
            return Stream.of(mapper.convertValue(job, Job.class));
        } catch (IllegalArgumentException e) {
            return Stream.empty();
        }
    }

    public static class On {
        private final Trigger trigger;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public On(@JsonProperty("pull_request") Trigger trigger) {
            this.trigger = Objects.requireNonNull(trigger, "trigger must not be null");
        }
    }

    public static class Trigger {
        private final List<String> paths;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Trigger(@JsonProperty("paths") List<String> paths) {
            this.paths = Objects.requireNonNull(paths, "paths must not be null");
        }
    }

    public static class Job {
        private final String uses;
        private final WorkflowInputs with;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Job(
                @JsonProperty("uses") String uses,
                @JsonProperty("with") WorkflowInputs with) {
            this.uses = Objects.requireNonNull(uses, "uses must not be null");
            this.with = Objects.requireNonNull(with, "with must not be null");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WorkflowInputs {
        private final String chunkId;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public WorkflowInputs(@JsonProperty("chunkId") String chunkId) {
            this.chunkId = Objects.requireNonNull(chunkId, "chunkId must not be null");
        }
    }
}
