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
package sleeper.clients.util.cdk;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public record CdkDeploy(List<String> arguments) implements CdkCommand {

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Stream<String> getCommand() {
        return Stream.of("deploy",
                "--require-approval", "never");
    }

    @Override
    public Stream<String> getArguments() {
        return arguments.stream();
    }

    public static final class Builder {
        private boolean ensureNewInstance;
        private boolean skipVersionCheck;
        private boolean deployPaused;
        private Path propertiesFile;

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

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public Builder propertiesFile(Path propertiesFile) {
            this.propertiesFile = propertiesFile;
            return this;
        }

        public CdkDeploy build() {
            return new CdkDeploy(buildArguments());
        }

        private List<String> buildArguments() {
            List<String> arguments = new ArrayList<>();
            if (ensureNewInstance) {
                arguments.addAll(List.of("-c", "newinstance=true"));
            }
            if (skipVersionCheck) {
                arguments.addAll(List.of("-c", "skipVersionCheck=true"));
            }
            if (deployPaused) {
                arguments.addAll(List.of("-c", "deployPaused=true"));
            }
            if (propertiesFile != null) {
                arguments.addAll(List.of("-c", "propertiesfile=" + propertiesFile));
            }
            return arguments;
        }
    }
}
