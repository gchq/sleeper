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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static sleeper.build.util.ValidationUtils.ignoreEmpty;

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
    private final List<String> changedPaths;

    private GitHubWorkflowRun(Builder builder) {
        status = ignoreEmpty(builder.status);
        conclusion = ignoreEmpty(builder.conclusion);
        runId = builder.runId;
        runUrl = ignoreEmpty(builder.runUrl);
        runStarted = builder.runStarted;
        commitSha = ignoreEmpty(builder.commitSha);
        commitMessage = ignoreEmpty(builder.commitMessage);
        changedPaths = Collections.unmodifiableList(Objects.requireNonNull(builder.changedPaths, "changedPaths must not be null"));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder withCommitSha(String commitSha) {
        return builder().commitSha(commitSha);
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

    public List<String> getChangedPaths() {
        return changedPaths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GitHubWorkflowRun that = (GitHubWorkflowRun) o;
        return Objects.equals(status, that.status) && Objects.equals(conclusion, that.conclusion)
                && Objects.equals(runId, that.runId) && Objects.equals(runUrl, that.runUrl)
                && Objects.equals(runStarted, that.runStarted) && Objects.equals(commitSha, that.commitSha)
                && Objects.equals(commitMessage, that.commitMessage) && changedPaths.equals(that.changedPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, conclusion, runId, runUrl, runStarted, commitSha, commitMessage, changedPaths);
    }

    @Override
    public String toString() {
        return "GitHubWorkflowRun{" +
                "status='" + status + '\'' +
                ", conclusion='" + conclusion + '\'' +
                ", runId=" + runId +
                ", runUrl='" + runUrl + '\'' +
                ", runStarted=" + runStarted +
                ", commitSha='" + commitSha + '\'' +
                ", commitMessage='" + commitMessage + '\'' +
                ", changedPaths=" + changedPaths +
                '}';
    }

    public static final class Builder {
        private String status;
        private String conclusion;
        private Long runId;
        private String runUrl;
        private Instant runStarted;
        private String commitSha;
        private String commitMessage;
        private List<String> changedPaths = Collections.emptyList();

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

        public Builder changedPaths(List<String> changedPaths) {
            this.changedPaths = changedPaths;
            return this;
        }

        public Builder changedPathsArray(String... changedPaths) {
            return changedPaths(Arrays.asList(changedPaths));
        }

        public GitHubWorkflowRun build() {
            return new GitHubWorkflowRun(this);
        }
    }
}
