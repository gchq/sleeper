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

import java.util.Objects;
import java.util.Properties;

public class GitHubBranch {

    private final String owner;
    private final String repository;
    private final String branch;

    private GitHubBranch(Builder builder) {
        this.owner = Objects.requireNonNull(builder.owner, "owner must not be null");
        this.repository = Objects.requireNonNull(builder.repository, "repository must not be null");
        this.branch = Objects.requireNonNull(builder.branch, "branch must not be null");
    }

    public String getOwnerAndRepository() {
        return owner + "/" + repository;
    }

    public String getBranch() {
        return branch;
    }

    public static GitHubBranch from(Properties properties) {
        return builder()
                .owner(properties.getProperty("owner"))
                .repository(properties.getProperty("repository"))
                .branch(properties.getProperty("branch"))
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
        GitHubBranch that = (GitHubBranch) o;
        return owner.equals(that.owner) && repository.equals(that.repository) && branch.equals(that.branch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, repository, branch);
    }

    @Override
    public String toString() {
        return "GitHubRepository{" +
                "owner='" + owner + '\'' +
                ", repository='" + repository + '\'' +
                ", branch='" + branch + '\'' +
                '}';
    }

    public static final class Builder {
        private String owner;
        private String repository;
        private String branch;

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

        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        public GitHubBranch build() {
            return new GitHubBranch(this);
        }
    }
}
