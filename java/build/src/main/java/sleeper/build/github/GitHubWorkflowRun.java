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
package sleeper.build.github;

import java.time.Instant;
import java.util.Date;

public class GitHubWorkflowRun {

    private static final String COMPLETED = "completed";
    private static final String SUCCESS = "success";
    private static final String IN_PROGRESS = "in_progress";

    private final String status;
    private final String conclusion;
    private final Long runId;
    private final String runUrl;
    private final Instant runStarted;
    private final String commitSha;
    private final String commitMessage;

    private GitHubWorkflowRun(Builder builder) {
        status = builder.status;
        conclusion = builder.conclusion;
        runId = builder.runId;
        runUrl = builder.runUrl;
        runStarted = builder.runStarted;
        commitSha = builder.commitSha;
        commitMessage = builder.commitMessage;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getStatus() {
        return status;
    }

    public String getConclusion() {
        return conclusion;
    }

    public Long getRunId() {
        return runId;
    }

    public String getRunUrl() {
        return runUrl;
    }

    public Instant getRunStarted() {
        return runStarted;
    }

    public String getCommitSha() {
        return commitSha;
    }

    public String getCommitMessage() {
        return commitMessage;
    }

    public static final class Builder {
        private String status;
        private String conclusion;
        private Long runId;
        private String runUrl;
        private Instant runStarted;
        private String commitSha;
        private String commitMessage;

        private Builder() {
        }

        public GitHubWorkflowRun success() {
            return status(COMPLETED).conclusion(SUCCESS).build();
        }

        public GitHubWorkflowRun inProgress() {
            return status(IN_PROGRESS).conclusion(null).build();
        }

        public GitHubWorkflowRun failure() {
            return status(COMPLETED).conclusion("failure").build();
        }

        public GitHubWorkflowRun cancelled() {
            return status(COMPLETED).conclusion("cancelled").build();
        }

        public GitHubWorkflowRun noBuild() {
            return status(null).conclusion(null).build();
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder conclusion(String conclusion) {
            this.conclusion = conclusion;
            return this;
        }

        public Builder runId(Long runId) {
            this.runId = runId;
            return this;
        }

        public Builder runUrl(String runUrl) {
            this.runUrl = runUrl;
            return this;
        }

        public Builder runStarted(Instant runStarted) {
            this.runStarted = runStarted;
            return this;
        }

        public Builder runStarted(Date runStarted) {
            return runStarted(runStarted.toInstant());
        }

        public Builder commitSha(String commitSha) {
            this.commitSha = commitSha;
            return this;
        }

        public Builder commitMessage(String commitMessage) {
            this.commitMessage = commitMessage;
            return this;
        }

        public GitHubWorkflowRun build() {
            return new GitHubWorkflowRun(this);
        }
    }
}
