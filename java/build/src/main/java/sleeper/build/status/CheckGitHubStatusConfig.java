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
import sleeper.build.chunks.ProjectChunks;
import sleeper.build.chunks.ProjectChunksYaml;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRuns;
import sleeper.build.github.api.GitHubWorkflowRunsImpl;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static sleeper.build.util.ValidationUtils.ignoreEmpty;

public class CheckGitHubStatusConfig {

    private static final long DEFAULT_RETRY_SECONDS = 60;
    private static final long DEFAULT_MAX_RETRIES = 60L * 15L / DEFAULT_RETRY_SECONDS; // Give up after 15 minutes

    private final String token;
    private final GitHubHead head;
    private final ProjectChunks chunks;
    private final long retrySeconds;
    private final long maxRetries;

    private CheckGitHubStatusConfig(Builder builder) {
        token = Objects.requireNonNull(ignoreEmpty(builder.token), "token must not be null");
        head = Objects.requireNonNull(builder.head, "head must not be null");
        chunks = Objects.requireNonNull(builder.chunks, "chunks must not be null");
        retrySeconds = builder.retrySeconds;
        maxRetries = builder.maxRetries;
    }

    public static CheckGitHubStatusConfig fromGitHubAndChunks(Properties gitHubProperties, ProjectChunks chunks) {
        return builder()
                .token(gitHubProperties.getProperty("token"))
                .head(GitHubHead.from(gitHubProperties))
                .chunks(chunks)
                .build();
    }

    public GitHubWorkflowRunsImpl gitHubWorkflowRuns() {
        return new GitHubWorkflowRunsImpl(token);
    }

    public ChunkStatuses checkStatus(GitHubWorkflowRuns runs) {
        return new CheckGitHubStatus(this, runs).checkStatus();
    }

    public WorkflowStatus checkStatusSingleWorkflow(GitHubWorkflowRuns workflowRuns, String workflow) {
        return new CheckGitHubStatus(this, workflowRuns).checkStatusSingleWorkflow(workflow);
    }

    public GitHubHead getHead() {
        return head;
    }

    public ProjectChunks getChunks() {
        return chunks;
    }

    public long getRetrySeconds() {
        return retrySeconds;
    }

    public long getMaxRetries() {
        return maxRetries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CheckGitHubStatusConfig fromGitHubAndChunks(Path gitHubProperties, Path chunksYaml) throws IOException {
        return fromGitHubAndChunks(
                loadProperties(gitHubProperties),
                ProjectChunksYaml.readPath(chunksYaml));
    }

    private static Properties loadProperties(Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path)) {
            properties.load(reader);
        }
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckGitHubStatusConfig that = (CheckGitHubStatusConfig) o;
        return retrySeconds == that.retrySeconds && maxRetries == that.maxRetries && token.equals(that.token) && head.equals(that.head) && chunks.equals(that.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, head, chunks, retrySeconds, maxRetries);
    }

    @Override
    public String toString() {
        return "ProjectConfiguration{" +
                "head=" + head +
                ", chunks=" + chunks +
                ", retrySeconds=" + retrySeconds +
                ", maxRetries=" + maxRetries +
                '}';
    }

    public static final class Builder {
        private String token;
        private GitHubHead head;
        private ProjectChunks chunks;
        private long retrySeconds = DEFAULT_RETRY_SECONDS;
        private long maxRetries = DEFAULT_MAX_RETRIES;

        private Builder() {
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder head(GitHubHead head) {
            this.head = head;
            return this;
        }

        public Builder chunks(ProjectChunks chunks) {
            this.chunks = chunks;
            return this;
        }

        public Builder chunks(List<ProjectChunk> chunks) {
            return chunks(new ProjectChunks(chunks));
        }

        public Builder retrySeconds(long retrySeconds) {
            this.retrySeconds = retrySeconds;
            return this;
        }

        public Builder maxRetries(long maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public CheckGitHubStatusConfig build() {
            return new CheckGitHubStatusConfig(this);
        }
    }
}
