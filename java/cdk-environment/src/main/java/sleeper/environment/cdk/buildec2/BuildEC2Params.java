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
package sleeper.environment.cdk.buildec2;

import sleeper.environment.cdk.util.AppContext;

import java.util.Objects;

public class BuildEC2Params {

    private final String repository;
    private final String fork;
    private final String branch;

    private BuildEC2Params(Builder builder) {
        repository = requireNonEmpty(builder.repository, "repository must not be empty");
        fork = requireNonEmpty(builder.fork, "fork must not be empty");
        branch = requireNonEmpty(builder.branch, "branch must not be empty");
    }

    public static BuildEC2Params from(AppContext context) {
        return builder()
                .repository(context.getStringOrDefault("repository", "sleeper"))
                .fork(context.getStringOrDefault("fork", "gchq"))
                .branch(context.getStringOrDefault("branch", "main"))
                .build();
    }

    String fillUserDataTemplate(String template) {
        return template.replace("${repository}", repository)
                .replace("${fork}", fork)
                .replace("${branch}", branch);
    }

    private static String requireNonEmpty(String value, String message) {
        Objects.requireNonNull(value, message);
        if (value.length() < 1) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String repository;
        private String fork;
        private String branch;

        private Builder() {
        }

        public Builder repository(String repository) {
            this.repository = repository;
            return this;
        }

        public Builder fork(String fork) {
            this.fork = fork;
            return this;
        }

        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        public BuildEC2Params build() {
            return new BuildEC2Params(this);
        }
    }
}
