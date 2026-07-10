/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.clients.util.cdk.CdkCommand;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.model.SleeperInternalCdkApp;

import java.nio.file.Path;
import java.util.Objects;

public class DeployInstanceRequest {

    private final SleeperInstanceConfiguration instanceConfig;
    private final CdkCommand cdkCommand;
    private final SleeperInternalCdkApp cdkApp;
    private Path propertiesFile;
    private Path configDir;

    private DeployInstanceRequest(Builder builder) {
        instanceConfig = Objects.requireNonNull(builder.instanceConfig, "instanceConfig must not be null");
        cdkCommand = Objects.requireNonNull(builder.cdkCommand, "cdkCommand must not be null");
        cdkApp = Objects.requireNonNull(builder.cdkApp, "cdkApp must not be null");
        propertiesFile = builder.propertiesFile;
        configDir = builder.configDir;
    }

    public static Builder builder() {
        return new Builder();
    }

    public SleeperInstanceConfiguration getInstanceConfig() {
        return instanceConfig;
    }

    public SleeperInternalCdkApp getCdkApp() {
        return cdkApp;
    }

    public CdkCommand getCdkCommand() {
        return cdkCommand;
    }

    public Path getPropertiesFile() {
        return propertiesFile;
    }

    public Path getConfigDir() {
        return configDir;
    }

    public static class Builder {
        private SleeperInstanceConfiguration instanceConfig;
        private CdkCommand cdkCommand;
        private SleeperInternalCdkApp cdkApp;
        private Path propertiesFile;
        private Path configDir;

        public Builder instanceConfig(SleeperInstanceConfiguration instanceConfig) {
            this.instanceConfig = instanceConfig;
            return this;
        }

        public Builder cdkCommand(CdkCommand cdkCommand) {
            this.cdkCommand = cdkCommand;
            return this;
        }

        public Builder cdkApp(SleeperInternalCdkApp cdkApp) {
            this.cdkApp = cdkApp;
            return this;
        }

        public Builder propertiesFile(Path propertiesFile) {
            this.propertiesFile = propertiesFile;
            return this;
        }

        public Builder configDir(Path configDir) {
            this.configDir = configDir;
            return this;
        }

        public DeployInstanceRequest build() {
            return new DeployInstanceRequest(this);
        }
    }

}
