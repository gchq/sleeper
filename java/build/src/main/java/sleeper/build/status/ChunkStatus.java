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
package sleeper.build.status;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;

import java.io.PrintStream;
import java.util.Objects;
import java.util.function.Function;

public class ChunkStatus {

    private final ProjectChunk chunk;
    private final GitHubWorkflowRun run;

    private ChunkStatus(Builder builder) {
        chunk = Objects.requireNonNull(builder.chunk, "chunk must not be null");
        run = Objects.requireNonNull(builder.run, "run must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void report(GitHubHead head, PrintStream out) {
        run.report(head, chunk.getName(), out);
    }

    public boolean isFailCheckWithHead(GitHubHead head) {
        return run.isFailCheckWithHead(head);
    }

    public boolean isWaitForOldBuildWithHead(GitHubHead head) {
        return run.isWaitForOldBuildWithHead(head);
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
