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
package sleeper.build.github.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import sleeper.build.github.GitHubWorkflowRun;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubWorkflowRunsResponse {

    private final List<Run> workflowRuns;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GitHubWorkflowRunsResponse(
            @JsonProperty("workflow_runs") List<Run> workflowRuns) {
        this.workflowRuns = workflowRuns == null ? Collections.emptyList() : workflowRuns;
    }

    public List<Run> getWorkflowRuns() {
        return workflowRuns;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Run {
        private final long id;
        private final String headSha;
        private final String status;
        private final String conclusion;
        private final String htmlUrl;
        private final Instant runStartedAt;
        private final Commit headCommit;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Run(
                @JsonProperty("id") long id,
                @JsonProperty("head_sha") String headSha,
                @JsonProperty("status") String status,
                @JsonProperty("conclusion") String conclusion,
                @JsonProperty("html_url") String htmlUrl,
                @JsonProperty("run_started_at") Instant runStartedAt,
                @JsonProperty("head_commit") Commit headCommit) {
            this.id = id;
            this.headSha = headSha;
            this.status = status;
            this.conclusion = conclusion;
            this.htmlUrl = htmlUrl;
            this.runStartedAt = runStartedAt;
            this.headCommit = headCommit == null ? new Commit(null) : headCommit;
        }

        public GitHubWorkflowRun toInternalRun() {
            return GitHubWorkflowRun.builder().runId(id)
                    .runUrl(Objects.toString(htmlUrl, null))
                    .runStarted(runStartedAt)
                    .commitSha(headSha)
                    .commitMessage(headCommit.message)
                    .status(Objects.toString(status, null))
                    .conclusion(Objects.toString(conclusion, null))
                    .build();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Commit {
        private final String message;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Commit(@JsonProperty("message") String message) {
            this.message = message;
        }
    }
}
