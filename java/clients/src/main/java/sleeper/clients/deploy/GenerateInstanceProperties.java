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

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperScheduleRule;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.requireNonEmpty;
import static sleeper.configuration.properties.InstanceProperties.getConfigBucketFromInstanceId;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

public class GenerateInstanceProperties {
    private final Supplier<String> accountSupplier;
    private final AwsRegionProvider regionProvider;
    private final String instanceId;
    private final String vpcId;
    private final String subnetId;
    private final Properties properties;
    private final Properties tagsProperties;

    private GenerateInstanceProperties(Builder builder) {
        accountSupplier = requireNonNull(builder.accountSupplier, "accountSupplier must not be null");
        regionProvider = requireNonNull(builder.regionProvider, "regionProvider must not be null");
        instanceId = requireNonEmpty(builder.instanceId, "instanceId must not be empty");
        vpcId = requireNonEmpty(builder.vpcId, "vpcId must not be empty");
        subnetId = requireNonEmpty(builder.subnetId, "subnetId must not be empty");
        properties = Optional.ofNullable(builder.properties).orElseGet(Properties::new);
        tagsProperties = Optional.ofNullable(builder.tagsProperties).orElseGet(Properties::new);
    }

    public static Builder builder() {
        return new Builder();
    }

    public InstanceProperties generate() {
        InstanceProperties instanceProperties = generateDefaultsFromInstanceId(properties, instanceId);
        instanceProperties.loadTags(tagsProperties);
        instanceProperties.set(ACCOUNT, accountSupplier.get());
        instanceProperties.set(REGION, regionProvider.getRegion().id());
        instanceProperties.set(VPC_ID, vpcId);
        instanceProperties.set(SUBNET, subnetId);
        return instanceProperties;
    }

    public static InstanceProperties generateTearDownDefaultsFromInstanceId(String instanceId) {
        InstanceProperties instanceProperties = generateDefaultsFromInstanceId(new Properties(), instanceId);
        SleeperScheduleRule.getCloudWatchRuleDefaults(instanceId)
                .forEach(rule -> instanceProperties.set(rule.getProperty(), rule.getPropertyValue()));
        return instanceProperties;
    }

    public static InstanceProperties generateDefaultsFromInstanceId(Properties properties, String instanceId) {
        InstanceProperties instanceProperties = new InstanceProperties(properties);
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, getConfigBucketFromInstanceId(instanceId));
        instanceProperties.set(JARS_BUCKET, String.format("sleeper-%s-jars", instanceId));
        instanceProperties.set(QUERY_RESULTS_BUCKET, String.format("sleeper-%s-query-results", instanceId));
        instanceProperties.set(ECR_COMPACTION_REPO, instanceId + "/compaction-job-execution");
        instanceProperties.set(ECR_INGEST_REPO, instanceId + "/ingest");
        instanceProperties.set(BULK_IMPORT_REPO, instanceId + "/bulk-import-runner");
        return instanceProperties;
    }

    public static final class Builder {
        private Supplier<String> accountSupplier;
        private AwsRegionProvider regionProvider;
        private String instanceId;
        private String vpcId;
        private String subnetId;
        private Properties properties;
        private Properties tagsProperties;

        private Builder() {
        }

        public Builder sts(AWSSecurityTokenService sts) {
            return accountSupplier(sts.getCallerIdentity(new GetCallerIdentityRequest())::getAccount);
        }

        public Builder accountSupplier(Supplier<String> accountSupplier) {
            this.accountSupplier = accountSupplier;
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetId(String subnetId) {
            this.subnetId = subnetId;
            return this;
        }

        public Builder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder tagsProperties(Properties tagsProperties) {
            this.tagsProperties = tagsProperties;
            return this;
        }

        public GenerateInstanceProperties build() {
            return new GenerateInstanceProperties(this);
        }
    }
}
