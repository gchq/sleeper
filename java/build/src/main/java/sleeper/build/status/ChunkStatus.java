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
package sleeper.build.status;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;

import static sleeper.build.util.ValidationUtils.ignoreEmpty;

public class ChunkStatus {

    private static final String COMPLETED = "completed";
    private static final String SUCCESS = "success";
    private static final String IN_PROGRESS = "in_progress";

    private final ProjectChunk chunk;
    private final String status;
    private final String conclusion;
    private final Long runId;
    private final String runUrl;
    private final Instant runStarted;
    private final String commitSha;
    private final String commitMessage;
    private final GitHubWorkflowRun run;

    private ChunkStatus(Builder builder) {
        chunk = Objects.requireNonNull(builder.chunk, "chunk must not be null");
        status = ignoreEmpty(builder.status);
        conclusion = ignoreEmpty(builder.conclusion);
        runId = builder.runId;
        runUrl = ignoreEmpty(builder.runUrl);
        runStarted = builder.runStarted;
        commitSha = ignoreEmpty(builder.commitSha);
        commitMessage = ignoreEmpty(builder.commitMessage);
        run = Objects.requireNonNull(builder.run);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void report(GitHubHead head, PrintStream out) {
        out.println();
        if (run.getConclusion() != null) {
            out.println(chunk.getName() + ": " + run.getStatus() + ", " + run.getConclusion());
        } else {
            out.println(chunk.getName() + ": " + run.getStatus());
        }
        if (run.getRunUrl() != null) {
            out.println("Run: " + run.getRunUrl());
        }
        if (run.getRunStarted() != null) {
            out.println("Started at: " + run.getRunStarted());
        }
        if (isRunForHead(head)) {
            out.println("Build is for current commit");
        } else {
            if (run.getCommitMessage() != null) {
                out.println("Message: " + run.getCommitMessage());
            }
            if (run.getCommitSha() != null) {
                out.println("Commit: " + run.getCommitSha());
            }
        }
    }

    public boolean isFailCheckWithHead(GitHubHead head) {
        return (COMPLETED.equals(run.getStatus()) && !SUCCESS.equals(run.getConclusion()))
                // Fail if there's an old build we want to wait for but the wait timed out
                || isWaitForOldBuildWithHead(head);
    }

    public boolean isWaitForOldBuildWithHead(GitHubHead head) {
        // If the run is not for the head commit, that should mean it is for a previous commit.
        // The GitHubProvider should ignore any builds for commits that are not in the history of the head commit.
        return IN_PROGRESS.equals(run.getStatus()) && !isRunForHead(head);
    }

    private boolean isRunForHead(GitHubHead head) {
        return head.getSha().equals(run.getCommitSha());
    }

    public String getChunkId() {
        return chunk.getId();
    }

    public ProjectChunk getChunk() {
        return chunk;
    }

    public Long getRunId() {
        return run.getRunId();
    }

    public String getRunUrl() {
        return run.getRunUrl();
    }

    public static Builder chunk(String chunk) {
        return builder().chunk(ProjectChunk.chunk(chunk).name(chunk).workflow(chunk).build());
    }

    public static Builder chunk(ProjectChunk chunk) {
        return builder().chunk(chunk);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChunkStatus that = (ChunkStatus) o;
        return chunk.equals(that.chunk) && run.equals(that.run);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunk, run);
    }

    @Override
    public String toString() {
        return "ChunkStatus{" +
                "chunk=" + chunk +
                ", run=" + run +
                '}';
    }

    public static final class Builder {
        private ProjectChunk chunk;
        private String status;
        private String conclusion;
        private Long runId;
        private String runUrl;
        private Instant runStarted;
        private String commitSha;
        private String commitMessage;
        public GitHubWorkflowRun run;

        private Builder() {
        }

        public Builder chunk(ProjectChunk chunk) {
            this.chunk = chunk;
            return this;
        }

        public ChunkStatus success() {
            return buildWithRun(GitHubWorkflowRun.Builder::success);
        }

        public ChunkStatus inProgressWithSha(String commitSha) {
            return buildWithRun(builder -> builder.commitSha(commitSha).inProgress());
        }

        public ChunkStatus failure() {
            return buildWithRun(GitHubWorkflowRun.Builder::failure);
        }

        public ChunkStatus cancelled() {
            return buildWithRun(GitHubWorkflowRun.Builder::cancelled);
        }

        public ChunkStatus noBuild() {
            return buildWithRun(GitHubWorkflowRun.Builder::noBuild);
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

        public Builder commitSha(String commitSha) {
            this.commitSha = commitSha;
            return this;
        }

        public Builder commitMessage(String commitMessage) {
            this.commitMessage = commitMessage;
            return this;
        }

        public Builder run(GitHubWorkflowRun run) {
            this.run = run;
            return this;
        }

        public ChunkStatus buildWithRun(Function<GitHubWorkflowRun.Builder, GitHubWorkflowRun> runConfig) {
            GitHubWorkflowRun.Builder builder = GitHubWorkflowRun.builder();
            return run(runConfig.apply(builder)).build();
        }

        public ChunkStatus build() {
            return new ChunkStatus(this);
        }
    }

}
