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

import java.util.Objects;
import java.util.Properties;

import static sleeper.build.util.ValidationUtils.ignoreEmpty;

public class GitHubHead {

    private final String owner;
    private final String repository;
    private final String branch;
    private final String sha;

    private GitHubHead(Builder builder) {
        this.owner = Objects.requireNonNull(ignoreEmpty(builder.owner), "owner must not be null");
        this.repository = Objects.requireNonNull(ignoreEmpty(builder.repository), "repository must not be null");
        this.branch = Objects.requireNonNull(ignoreEmpty(builder.branch), "branch must not be null");
        this.sha = Objects.requireNonNull(ignoreEmpty(builder.sha), "sha must not be null");
    }

    public String getOwnerAndRepository() {
        return owner + "/" + repository;
    }

    public String getBranch() {
        return branch;
    }

    public String getSha() {
        return sha;
    }

    public static GitHubHead from(Properties properties) {
        return builder()
                .ownerAndRepository(properties.getProperty("repository"))
                .branch(properties.getProperty("head.branch"))
                .sha(properties.getProperty("head.sha"))
                .build();
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
        GitHubHead that = (GitHubHead) o;
        return owner.equals(that.owner) && repository.equals(that.repository) && branch.equals(that.branch) && sha.equals(that.sha);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, repository, branch, sha);
    }

    @Override
    public String toString() {
        return "GitHubHead{" +
                "owner='" + owner + '\'' +
                ", repository='" + repository + '\'' +
                ", branch='" + branch + '\'' +
                ", sha='" + sha + '\'' +
                '}';
    }

    public static final class Builder {
        private String owner;
        private String repository;
        private String branch;
        private String sha;

        private Builder() {
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder repository(String repository) {
            this.repository = repository;
            return this;
        }

        public Builder ownerAndRepository(String ownerAndRepository) {
            String[] parts = ownerAndRepository.split("/");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Owner and repository must be separated by a slash, found: " + ownerAndRepository);
            }
            return owner(parts[0]).repository(parts[1]);
        }

        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        public Builder sha(String sha) {
            this.sha = sha;
            return this;
        }

        public GitHubHead build() {
            return new GitHubHead(this);
        }
    }
}
