/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.systemtest.drivers.instance;

import sleeper.clients.deploy.DeployInstanceConfiguration;

public class SystemTestInstanceConfiguration {
    private final DeployInstanceConfiguration deployConfig;
    private final boolean useSystemTestIngestSourceBucket;

    private SystemTestInstanceConfiguration(Builder builder) {
        deployConfig = builder.deployConfig;
        useSystemTestIngestSourceBucket = builder.useSystemTestIngestSourceBucket;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SystemTestInstanceConfiguration usingSystemTestDefaults(DeployInstanceConfiguration deployConfig) {
        return builder().deployConfig(deployConfig).build();
    }

    public DeployInstanceConfiguration getDeployConfig() {
        return deployConfig;
    }

    public boolean shouldUseSystemTestIngestSourceBucket() {
        return useSystemTestIngestSourceBucket;
    }

    public static final class Builder {
        private DeployInstanceConfiguration deployConfig;
        private boolean useSystemTestIngestSourceBucket = true;

        private Builder() {
        }

        public Builder deployConfig(DeployInstanceConfiguration deployConfig) {
            this.deployConfig = deployConfig;
            return this;
        }

        public Builder useSystemTestIngestSourceBucket(boolean useSystemTestIngestSourceBucket) {
            this.useSystemTestIngestSourceBucket = useSystemTestIngestSourceBucket;
            return this;
        }

        public SystemTestInstanceConfiguration build() {
            return new SystemTestInstanceConfiguration(this);
        }
    }
}
