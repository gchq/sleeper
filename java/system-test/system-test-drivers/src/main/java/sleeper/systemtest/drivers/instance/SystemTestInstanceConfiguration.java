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

import sleeper.configuration.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_ROLE;

public class SystemTestInstanceConfiguration {
    private final String identifier;
    private final Supplier<DeployInstanceConfiguration> deployConfig;
    private final boolean useSystemTestIngestSourceBucket;

    private SystemTestInstanceConfiguration(Builder builder) {
        identifier = builder.identifier;
        deployConfig = builder.deployConfig;
        useSystemTestIngestSourceBucket = builder.useSystemTestIngestSourceBucket;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SystemTestInstanceConfiguration usingSystemTestDefaults(
            String identifier, Supplier<DeployInstanceConfiguration> deployConfig) {
        return builder().identifier(identifier).deployConfig(deployConfig).build();
    }

    public static SystemTestInstanceConfiguration noSourceBucket(
            String identifier, Supplier<DeployInstanceConfiguration> deployConfig) {
        return builder().identifier(identifier).deployConfig(deployConfig)
                .useSystemTestIngestSourceBucket(false).build();
    }

    public DeployInstanceConfiguration buildDeployConfig(
            SystemTestParameters parameters, SystemTestDeploymentContext systemTest) {
        DeployInstanceConfiguration configuration = buildDeployConfig(parameters);
        InstanceProperties properties = configuration.getInstanceProperties();
        if (shouldUseSystemTestIngestSourceBucket()) {
            properties.set(INGEST_SOURCE_BUCKET, systemTest.getSystemTestBucketName());
        }
        properties.set(INGEST_SOURCE_ROLE, systemTest.getSystemTestWriterRoleName());
        return configuration;
    }

    public DeployInstanceConfiguration buildDeployConfig(SystemTestParameters parameters) {
        DeployInstanceConfiguration configuration = deployConfig.get();
        parameters.setRequiredProperties(configuration);
        return configuration;
    }

    public String getIdentifier() {
        return identifier;
    }

    public boolean shouldUseSystemTestIngestSourceBucket() {
        return useSystemTestIngestSourceBucket;
    }

    public static final class Builder {
        private Supplier<DeployInstanceConfiguration> deployConfig;
        private boolean useSystemTestIngestSourceBucket = true;
        private String identifier;

        private Builder() {
        }

        public Builder identifier(String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder deployConfig(Supplier<DeployInstanceConfiguration> deployConfig) {
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
