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

package sleeper.systemtest.dsl.instance;

import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static sleeper.core.deploy.SleeperScheduleRule.COMPACTION_JOB_CREATION;
import static sleeper.core.deploy.SleeperScheduleRule.COMPACTION_TASK_CREATION;
import static sleeper.core.deploy.SleeperScheduleRule.INGEST_TASK_CREATION;
import static sleeper.core.deploy.SleeperScheduleRule.QUERY_WARM_LAMBDA;
import static sleeper.core.deploy.SleeperScheduleRule.TABLE_METRICS;
import static sleeper.core.deploy.SleeperScheduleRule.TRANSACTION_LOG_SNAPSHOT_CREATION;
import static sleeper.core.deploy.SleeperScheduleRule.TRANSACTION_LOG_SNAPSHOT_DELETION;
import static sleeper.core.deploy.SleeperScheduleRule.TRANSACTION_LOG_TRANSACTION_DELETION;
import static sleeper.core.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class SystemTestInstanceConfiguration {
    private final String shortName;
    private final Supplier<DeployInstanceConfiguration> deployConfig;
    private final boolean useSystemTestIngestSourceBucket;
    private final Set<SleeperScheduleRule> enableSchedules;

    private SystemTestInstanceConfiguration(Builder builder) {
        shortName = builder.shortName;
        deployConfig = builder.deployConfig;
        useSystemTestIngestSourceBucket = builder.useSystemTestIngestSourceBucket;
        enableSchedules = builder.enableSchedules;
        // Combines with SystemTestParameters.shortTestId and a hyphen to create an instance ID within maximum length
        if (shortName.length() > 6) {
            throw new IllegalArgumentException("Instance shortName must not be longer than 6 characters");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SystemTestInstanceConfiguration usingSystemTestDefaults(
            String shortName, Supplier<DeployInstanceConfiguration> deployConfig) {
        return builder().shortName(shortName).deployConfig(deployConfig).build();
    }

    public static SystemTestInstanceConfiguration noSourceBucket(
            String shortName, Supplier<DeployInstanceConfiguration> deployConfig) {
        return builder().shortName(shortName).deployConfig(deployConfig)
                .useSystemTestIngestSourceBucket(false).build();
    }

    public DeployInstanceConfiguration buildDeployConfig(
            SystemTestParameters parameters, DeployedSystemTestResources systemTest) {
        DeployInstanceConfiguration configuration = buildDeployConfig(parameters);
        InstanceProperties properties = configuration.getInstanceProperties();
        if (shouldUseSystemTestIngestSourceBucket()) {
            properties.set(INGEST_SOURCE_BUCKET, systemTest.getSystemTestBucketName());
        }
        return configuration;
    }

    public DeployInstanceConfiguration buildDeployConfig(SystemTestParameters parameters) {
        DeployInstanceConfiguration configuration = deployConfig.get();
        parameters.setRequiredProperties(configuration);
        return configuration;
    }

    public String getShortName() {
        return shortName;
    }

    public boolean shouldUseSystemTestIngestSourceBucket() {
        return useSystemTestIngestSourceBucket;
    }

    public boolean isEnable(SleeperScheduleRule.InstanceRule rule) {
        return enableSchedules.contains(rule.getRule());
    }

    public static final class Builder {
        private Supplier<DeployInstanceConfiguration> deployConfig;
        private boolean useSystemTestIngestSourceBucket = true;
        private Set<SleeperScheduleRule> enableSchedules = Stream.of(
                QUERY_WARM_LAMBDA,
                TRANSACTION_LOG_SNAPSHOT_CREATION,
                TRANSACTION_LOG_SNAPSHOT_DELETION,
                TRANSACTION_LOG_TRANSACTION_DELETION,
                TABLE_METRICS,
                INGEST_TASK_CREATION,
                COMPACTION_TASK_CREATION,
                COMPACTION_JOB_CREATION).collect(toSet());
        private String shortName;

        private Builder() {
        }

        public Builder shortName(String shortName) {
            this.shortName = shortName;
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

        public Builder enableSchedules(List<SleeperScheduleRule> enableSchedules) {
            this.enableSchedules.addAll(enableSchedules);
            return this;
        }

        public Builder disableSchedules(List<SleeperScheduleRule> disableSchedules) {
            this.enableSchedules.removeAll(disableSchedules);
            return this;
        }

        public SystemTestInstanceConfiguration build() {
            return new SystemTestInstanceConfiguration(this);
        }
    }
}
