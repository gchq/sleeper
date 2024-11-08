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
package sleeper.core.deploy;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.core.properties.instance.IngestProperty.ECR_INGEST_REPO;

/**
 * Populates instance properties when deploying a new instance or when tearing down an instance without the properties.
 */
public class PopulateInstanceProperties {
    public static final Logger LOGGER = LoggerFactory.getLogger(PopulateInstanceProperties.class);

    private final Supplier<String> accountSupplier;
    private final Supplier<String> regionIdSupplier;
    private final String instanceId;
    private final String vpcId;
    private final String subnetIds;
    private final Consumer<InstanceProperties> extraInstanceProperties;

    private PopulateInstanceProperties(Builder builder) {
        accountSupplier = Objects.requireNonNull(builder.accountSupplier, "accountSupplier must not be null");
        regionIdSupplier = Objects.requireNonNull(builder.regionIdSupplier, "regionIdSupplier must not be null");
        instanceId = ObjectUtils.requireNonEmpty(builder.instanceId, "instanceId must not be empty");
        vpcId = ObjectUtils.requireNonEmpty(builder.vpcId, "vpcId must not be empty");
        subnetIds = ObjectUtils.requireNonEmpty(builder.subnetIds, "subnetIds must not be empty");
        extraInstanceProperties = Objects.requireNonNull(builder.extraInstanceProperties, "extraInstanceProperties must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Sets instance properties when deploying a new instance.
     *
     * @param  properties the properties specified by the user
     * @return            the populated properties
     */
    public InstanceProperties populate(InstanceProperties properties) {
        populateDefaultsFromInstanceId(properties, instanceId);
        Properties tagsProperties = properties.getTagsProperties();
        tagsProperties.setProperty("InstanceID", instanceId);
        properties.loadTags(tagsProperties);
        properties.set(ACCOUNT, accountSupplier.get());
        properties.set(REGION, regionIdSupplier.get());
        properties.set(VPC_ID, vpcId);
        properties.set(SUBNETS, subnetIds);
        extraInstanceProperties.accept(properties);
        return properties;
    }

    /**
     * Generates instance properties when tearing down an instance without the properties.
     *
     * @param  instanceId the instance ID
     * @return            the dummy populated properties
     */
    public static InstanceProperties generateTearDownDefaultsFromInstanceId(String instanceId) {
        InstanceProperties instanceProperties = populateDefaultsFromInstanceId(new InstanceProperties(), instanceId);
        instanceProperties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
        instanceProperties.set(CONFIG_BUCKET, InstanceProperties.getConfigBucketFromInstanceId(instanceId));
        instanceProperties.set(QUERY_RESULTS_BUCKET, String.format("sleeper-%s-query-results", instanceId));
        SleeperScheduleRule.getCloudWatchRuleDefaults(instanceId)
                .forEach(rule -> instanceProperties.set(rule.getProperty(), rule.getPropertyValue()));
        return instanceProperties;
    }

    /**
     * Sets instance properties when deploying a new instance against LocalStack.
     *
     * @param  properties the properties specified by the user
     * @return            the populated properties
     */
    public static InstanceProperties populateDefaultsFromInstanceId(InstanceProperties properties, String instanceId) {
        properties.set(ID, instanceId);
        properties.set(JARS_BUCKET, String.format("sleeper-%s-jars", instanceId));
        String ecrPrefix = Optional.ofNullable(properties.get(ECR_REPOSITORY_PREFIX)).orElse(instanceId);
        properties.set(ECR_COMPACTION_REPO, ecrPrefix + "/compaction-job-execution");
        properties.set(ECR_INGEST_REPO, ecrPrefix + "/ingest");
        properties.set(BULK_IMPORT_REPO, ecrPrefix + "/bulk-import-runner");
        properties.set(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, ecrPrefix + "/bulk-import-runner-emr-serverless");
        return properties;
    }

    /**
     * Builds instances of this class.
     */
    public static final class Builder {
        private Supplier<String> accountSupplier;
        private Supplier<String> regionIdSupplier;
        private String instanceId;
        private String vpcId;
        private String subnetIds;
        private Consumer<InstanceProperties> extraInstanceProperties = properties -> {
        };

        private Builder() {
        }

        /**
         * Sets the AWS code to retrieve the account to deploy to.
         *
         * @param  accountSupplier the code
         * @return                 this builder
         */
        public Builder accountSupplier(Supplier<String> accountSupplier) {
            this.accountSupplier = accountSupplier;
            return this;
        }

        /**
         * Sets the AWS code to retrieve the ID of the region to deploy to.
         *
         * @param  regionIdSupplier the code
         * @return                  this builder
         */
        public Builder regionIdSupplier(Supplier<String> regionIdSupplier) {
            this.regionIdSupplier = regionIdSupplier;
            return this;
        }

        /**
         * Sets the instance ID of the Sleeper instance to deploy.
         *
         * @param  instanceId the instance ID
         * @return            this builder
         */
        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        /**
         * Sets the ID of the VPC to deploy to.
         *
         * @param  vpcId the VPC ID
         * @return       this builder
         */
        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        /**
         * Sets the comma-separated IDs of the subnets to deploy to.
         *
         * @param  subnetIds the subnet IDs (comma-separated)
         * @return           this builder
         */
        public Builder subnetIds(String subnetIds) {
            this.subnetIds = subnetIds;
            return this;
        }

        /**
         * Sets any extra instance properties that should be set for the instance.
         *
         * @param  extraInstanceProperties the function to set the extra properties
         * @return                         this builder
         */
        public Builder extraInstanceProperties(Consumer<InstanceProperties> extraInstanceProperties) {
            this.extraInstanceProperties = extraInstanceProperties;
            return this;
        }

        public PopulateInstanceProperties build() {
            return new PopulateInstanceProperties(this);
        }
    }
}
