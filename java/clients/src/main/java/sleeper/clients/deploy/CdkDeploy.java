/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.deploy;

import java.util.stream.Stream;

public class CdkDeploy implements CdkCommand {
    private final boolean ensureNewInstance;
    private final boolean skipVersionCheck;

    private CdkDeploy(Builder builder) {
        ensureNewInstance = builder.ensureNewInstance;
        skipVersionCheck = builder.skipVersionCheck;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CdkDeploy updateProperties() {
        return builder().ensureNewInstance(false).skipVersionCheck(false).build();
    }

    @Override
    public Stream<String> getCommand() {
        return Stream.of("deploy",
                "--require-approval", "never");
    }

    @Override
    public Stream<String> getArguments() {
        return Stream.concat(getNewInstanceArguments(), getSkipVersionCheckArguments());
    }

    private Stream<String> getNewInstanceArguments() {
        if (ensureNewInstance) {
            return Stream.of("-c", "newinstance=true");
        } else {
            return Stream.empty();
        }
    }

    private Stream<String> getSkipVersionCheckArguments() {
        if (skipVersionCheck) {
            return Stream.of("-c", "skipVersionCheck=true");
        } else {
            return Stream.empty();
        }
    }


    public static final class Builder {
        private boolean ensureNewInstance;
        private boolean skipVersionCheck;

        private Builder() {
        }

        public Builder ensureNewInstance(boolean ensureNewInstance) {
            this.ensureNewInstance = ensureNewInstance;
            return this;
        }

        public Builder skipVersionCheck(boolean skipVersionCheck) {
            this.skipVersionCheck = skipVersionCheck;
            return this;
        }

        public CdkDeploy build() {
            return new CdkDeploy(this);
        }
    }
}
