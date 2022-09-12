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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class ProjectConfiguration {

    private final String token;
    private final GitHubBranch branch;
    private final List<ProjectChunk> chunks;

    private ProjectConfiguration(Builder builder) {
        token = Objects.requireNonNull(builder.token, "token must not be null");
        branch = Objects.requireNonNull(builder.branch, "branch must not be null");
        chunks = Objects.requireNonNull(builder.chunks, "chunks must not be null");
    }

    public static ProjectConfiguration from(Properties properties) {
        return builder()
                .token(properties.getProperty("token"))
                .branch(GitHubBranch.from(properties))
                .chunks(ProjectChunk.listFrom(properties))
                .build();
    }

    public ChunksStatus checkStatus() throws IOException {
        return checkStatus(new GitHubProvider(token));
    }

    public ChunksStatus checkStatus(GitHubProvider gitHub) {
        return ChunksStatus.chunks(chunks.stream()
                .map(chunk -> gitHub.workflowStatus(branch, chunk))
                .collect(Collectors.toList()));
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
        private GitHubBranch branch;
        private List<ProjectChunk> chunks;

        private Builder() {
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder branch(GitHubBranch branch) {
            this.branch = branch;
            return this;
        }

        public Builder chunks(List<ProjectChunk> chunks) {
            this.chunks = chunks;
            return this;
        }

        public ProjectConfiguration build() {
            return new ProjectConfiguration(this);
        }
    }
}
