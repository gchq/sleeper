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
package sleeper.clients.util.cdk;

import java.util.Objects;
import java.util.stream.Stream;

public class CdkDeploy implements CdkCommand {
    private final boolean ensureNewInstance;
    private final boolean skipVersionCheck;
    private final boolean deployPaused;

    private CdkDeploy(Builder builder) {
        ensureNewInstance = builder.ensureNewInstance;
        skipVersionCheck = builder.skipVersionCheck;
        deployPaused = builder.deployPaused;
    }

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
        return Stream.of(getNewInstanceArguments(), getSkipVersionCheckArguments(), getDeployPausedArguments())
                .flatMap(s -> s);
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

    private Stream<String> getDeployPausedArguments() {
        if (deployPaused) {
            return Stream.of("-c", "deployPaused=true");
        } else {
            return Stream.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CdkDeploy cdkDeploy = (CdkDeploy) o;
        return ensureNewInstance == cdkDeploy.ensureNewInstance
                && skipVersionCheck == cdkDeploy.skipVersionCheck
                && deployPaused == cdkDeploy.deployPaused;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ensureNewInstance, skipVersionCheck, deployPaused);
    }

    @Override
    public String toString() {
        return "CdkDeploy{" +
                "ensureNewInstance=" + ensureNewInstance +
                ", skipVersionCheck=" + skipVersionCheck +
                ", deployPaused=" + deployPaused +
                '}';
    }

    public static final class Builder {
        private boolean ensureNewInstance;
        private boolean skipVersionCheck;
        private boolean deployPaused;

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

        public CdkDeploy build() {
            return new CdkDeploy(this);
        }
    }
}
