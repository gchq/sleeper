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
package sleeper.build.github;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
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
    private final String branch;
    private final String commitSha;
    private final String commitMessage;
    private final List<String> pathsChangedSinceThisRun;

    private GitHubWorkflowRun(Builder builder) {
        status = ignoreEmpty(builder.status);
        conclusion = ignoreEmpty(builder.conclusion);
        runId = builder.runId;
        runUrl = ignoreEmpty(builder.runUrl);
        runStarted = builder.runStarted;
        branch = ignoreEmpty(builder.branch);
        commitSha = ignoreEmpty(builder.commitSha);
        commitMessage = ignoreEmpty(builder.commitMessage);
        pathsChangedSinceThisRun = Collections.unmodifiableList(Objects.requireNonNull(builder.pathsChangedSinceThisRun, "pathsChangedSinceThisRun must not be null"));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder withCommitSha(String commitSha) {
        return builder().commitSha(commitSha);
    }

    public void report(GitHubHead head, String buildName, PrintStream out) {
        out.println();
        if (conclusion != null) {
            out.println(buildName + ": " + status + ", " + conclusion);
        } else {
            out.println(buildName + ": " + status);
        }
        if (runUrl != null) {
            out.println("Run: " + runUrl);
        }
        if (runStarted != null) {
            out.println("Started at: " + runStarted);
        }
        if (isSameCommit(head)) {
            out.println("Build is for current commit");
        } else {
            if (commitMessage != null) {
                out.println("Message: " + commitMessage);
            }
            if (commitSha != null) {
                out.println("Commit: " + commitSha);
            }
        }
    }

    public boolean isFailCheckWithHead(GitHubHead head) {
        if (COMPLETED.equals(status) && !SUCCESS.equals(conclusion)) {
            return true;
        } else {
            // Fail if there's an old build we want to wait for but the wait timed out
            return isWaitForOldBuildWithHead(head);
        }
    }

    public boolean isWaitForOldBuildWithHead(GitHubHead head) {
        // If the run is not for the head commit, that should mean it is for a previous commit.
        // The GitHubProvider should ignore any builds for commits that are not in the history of the head commit.
        return IN_PROGRESS.equals(status) && !isSameCommit(head);
    }

    public boolean isSameCommit(GitHubHead head) {
        return Objects.equals(head.getSha(), commitSha);
    }

    public boolean isSameBranch(GitHubHead head) {
        return Objects.equals(head.getBranch(), branch);
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

    public List<String> getPathsChangedSinceThisRun() {
        return pathsChangedSinceThisRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GitHubWorkflowRun that = (GitHubWorkflowRun) o;
        return Objects.equals(status, that.status) && Objects.equals(conclusion, that.conclusion)
                && Objects.equals(runId, that.runId) && Objects.equals(runUrl, that.runUrl)
                && Objects.equals(runStarted, that.runStarted) && Objects.equals(commitSha, that.commitSha)
                && Objects.equals(commitMessage, that.commitMessage) && pathsChangedSinceThisRun.equals(that.pathsChangedSinceThisRun);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, conclusion, runId, runUrl, runStarted, commitSha, commitMessage, pathsChangedSinceThisRun);
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
                ", changedPaths=" + pathsChangedSinceThisRun +
                '}';
    }

    public static final class Builder {
        private String status;
        private String conclusion;
        private Long runId;
        private String runUrl;
        private Instant runStarted;
        private String branch;
        private String commitSha;
        private String commitMessage;
        private List<String> pathsChangedSinceThisRun = Collections.emptyList();

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

        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        public Builder commitSha(String commitSha) {
            this.commitSha = commitSha;
            return this;
        }

        public Builder commitMessage(String commitMessage) {
            this.commitMessage = commitMessage;
            return this;
        }

        public Builder pathsChangedSinceThisRun(List<String> changedPaths) {
            this.pathsChangedSinceThisRun = changedPaths;
            return this;
        }

        public Builder pathsChangedSinceThisRunArray(String... changedPaths) {
            return pathsChangedSinceThisRun(Arrays.asList(changedPaths));
        }

        public GitHubWorkflowRun build() {
            return new GitHubWorkflowRun(this);
        }
    }
}
