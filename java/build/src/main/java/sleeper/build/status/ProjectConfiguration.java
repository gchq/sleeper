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

import sleeper.build.status.github.GitHubProvider;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static sleeper.build.status.ValidationUtils.ignoreEmpty;

public class ProjectConfiguration {

    private final String token;
    private final GitHubHead head;
    private final List<ProjectChunk> chunks;
    private final long retrySeconds;
    private final long maxRetries;

    private ProjectConfiguration(Builder builder) {
        token = Objects.requireNonNull(ignoreEmpty(builder.token), "token must not be null");
        head = Objects.requireNonNull(builder.head, "head must not be null");
        chunks = Objects.requireNonNull(builder.chunks, "chunks must not be null");
        retrySeconds = builder.retrySeconds;
        maxRetries = builder.maxRetries;
    }

    public static ProjectConfiguration from(Properties properties) {
        return builder()
                .token(properties.getProperty("token"))
                .head(GitHubHead.from(properties))
                .chunks(ProjectChunk.listFrom(properties))
                .build();
    }

    public ChunksStatus checkStatus() throws IOException {
        return checkStatus(new GitHubProvider(token));
    }

    public ChunksStatus checkStatus(GitHubProvider gitHub) {
        return new CheckGitHubStatus(this, gitHub).checkStatus();
    }

    public GitHubHead getHead() {
        return head;
    }

    public List<ProjectChunk> getChunks() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectConfiguration that = (ProjectConfiguration) o;
        return token.equals(that.token) && chunks.equals(that.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, chunks);
    }

    @Override
    public String toString() {
        return "GitHubProperties{" +
                "token='" + token + '\'' +
                ", chunks=" + chunks +
                '}';
    }

    public static final class Builder {
        private String token;
        private GitHubHead head;
        private List<ProjectChunk> chunks;
        private long retrySeconds = 10;
        private long maxRetries = 60L * 15L / retrySeconds; // Retry after 15 minutes of waiting

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

        public Builder chunks(List<ProjectChunk> chunks) {
            this.chunks = chunks;
            return this;
        }

        public Builder retrySeconds(long retrySeconds) {
            this.retrySeconds = retrySeconds;
            return this;
        }

        public Builder maxRetries(long maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public ProjectConfiguration build() {
            return new ProjectConfiguration(this);
        }
    }
}
