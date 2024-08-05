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

package sleeper.clients.deploy;

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;

import sleeper.configuration.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.requireNonEmpty;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_GPU_REPO;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.instance.IngestProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.instance.InstanceProperties.getConfigBucketFromInstanceId;

public class PopulateInstanceProperties {
    private final Supplier<String> accountSupplier;
    private final AwsRegionProvider regionProvider;
    private final String instanceId;
    private final String vpcId;
    private final String subnetIds;
    private final InstanceProperties properties;
    private final Properties tagsProperties;

    private PopulateInstanceProperties(Builder builder) {
        accountSupplier = requireNonNull(builder.accountSupplier, "accountSupplier must not be null");
        regionProvider = requireNonNull(builder.regionProvider, "regionProvider must not be null");
        instanceId = requireNonEmpty(builder.instanceId, "instanceId must not be empty");
        vpcId = requireNonEmpty(builder.vpcId, "vpcId must not be empty");
        subnetIds = requireNonEmpty(builder.subnetIds, "subnetIds must not be empty");
        properties = Optional.ofNullable(builder.properties).orElseGet(InstanceProperties::new);
        tagsProperties = Optional.ofNullable(builder.tagsProperties).orElseGet(properties::getTagsProperties);
    }

    public static Builder builder() {
        return new Builder();
    }

    public InstanceProperties populate() {
        InstanceProperties instanceProperties = populateDefaultsFromInstanceId(properties, instanceId);
        tagsProperties.setProperty("InstanceID", instanceId);
        instanceProperties.loadTags(tagsProperties);
        instanceProperties.set(ACCOUNT, accountSupplier.get());
        instanceProperties.set(REGION, regionProvider.getRegion().id());
        instanceProperties.set(VPC_ID, vpcId);
        instanceProperties.set(SUBNETS, subnetIds);
        return instanceProperties;
    }

    public static InstanceProperties generateTearDownDefaultsFromInstanceId(String instanceId) {
        InstanceProperties instanceProperties = populateDefaultsFromInstanceId(new InstanceProperties(), instanceId);
        instanceProperties.set(CONFIG_BUCKET, getConfigBucketFromInstanceId(instanceId));
        instanceProperties.set(QUERY_RESULTS_BUCKET, String.format("sleeper-%s-query-results", instanceId));
        SleeperScheduleRule.getCloudWatchRuleDefaults(instanceId)
                .forEach(rule -> instanceProperties.set(rule.getProperty(), rule.getPropertyValue()));
        return instanceProperties;
    }

    public static InstanceProperties populateDefaultsFromInstanceId(InstanceProperties properties, String instanceId) {
        String ecrPrefix = Optional.ofNullable(properties.get(ECR_REPOSITORY_PREFIX)).orElse(instanceId);
        properties.set(ID, instanceId);
        properties.set(JARS_BUCKET, String.format("sleeper-%s-jars", instanceId));
        properties.set(QUERY_RESULTS_BUCKET, String.format("sleeper-%s-query-results", instanceId));
        properties.set(ECR_COMPACTION_GPU_REPO, ecrPrefix + "/compaction-gpu");
        properties.set(ECR_COMPACTION_REPO, ecrPrefix + "/compaction-job-execution");
        properties.set(ECR_INGEST_REPO, ecrPrefix + "/ingest");
        properties.set(BULK_IMPORT_REPO, ecrPrefix + "/bulk-import-runner");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, ecrPrefix + "/bulk-import-runner-emr-serverless");
        return properties;
    }

    public static final class Builder {
        private Supplier<String> accountSupplier;
        private AwsRegionProvider regionProvider;
        private String instanceId;
        private String vpcId;
        private String subnetIds;
        private InstanceProperties properties;
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

        public Builder subnetIds(String subnetIds) {
            this.subnetIds = subnetIds;
            return this;
        }

        public Builder instanceProperties(InstanceProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder instanceProperties(Path propertiesPath) {
            this.properties = new InstanceProperties(loadProperties(propertiesPath));
            return this;
        }

        public Builder tagsProperties(Properties tagsProperties) {
            this.tagsProperties = tagsProperties;
            return this;
        }

        public Builder deployInstanceConfig(DeployInstanceConfiguration deployInstanceConfig) {
            return instanceProperties(deployInstanceConfig.getInstanceProperties())
                    .tagsProperties(deployInstanceConfig.getInstanceProperties().getTagsProperties());
        }

        public PopulateInstanceProperties build() {
            return new PopulateInstanceProperties(this);
        }
    }
}
