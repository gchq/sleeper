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
package sleeper.cdk;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.internal.BucketUtils;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.artefacts.SleeperJarVersionIdsCache;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.networking.SleeperNetworking;
import sleeper.cdk.networking.SleeperNetworkingProvider;
import sleeper.cdk.util.CdkContext;
import sleeper.cdk.util.MismatchedVersionException;
import sleeper.cdk.util.NewInstanceValidator;
import sleeper.core.SleeperVersion;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ARTEFACTS_DEPLOYMENT_ID;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

/**
 * Configuration to deploy a Sleeper instance with the CDK.
 */
public class SleeperInstanceProps {

    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tableProperties;
    private final SleeperJarVersionIdsCache jars;
    private final SleeperArtefacts artefacts;
    private final SleeperNetworkingProvider networkingProvider;
    private final String version;
    private final boolean validateProperties;
    private final boolean deployPaused;

    private SleeperInstanceProps(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
        jars = builder.jars;
        artefacts = builder.artefacts;
        networkingProvider = builder.networkingProvider;
        version = builder.version;
        validateProperties = builder.validateProperties;
        deployPaused = builder.deployPaused;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder with default settings to deploy an instance.
     *
     * @param  configuration the configuration of the instance to deploy
     * @param  s3Client      the S3 client, to scan for jars to deploy and validate the current state
     * @param  dynamoClient  the DynamoDB client, to validate the current state
     * @return               the builder
     */
    public static Builder builder(SleeperInstanceConfiguration configuration, S3Client s3Client, DynamoDbClient dynamoClient) {
        return builder(configuration.getInstanceProperties(), s3Client, dynamoClient)
                .tableProperties(configuration.getTableProperties());
    }

    /**
     * Creates a builder with default settings to deploy an instance.
     *
     * @param  instanceProperties the configuration of the instance to deploy
     * @param  s3Client           the S3 client, to scan for jars to deploy and validate the current state
     * @param  dynamoClient       the DynamoDB client, to validate the current state
     * @return                    the builder
     */
    public static Builder builder(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoClient) {
        return builder()
                .instanceProperties(instanceProperties)
                .jars(SleeperJarVersionIdsCache.from(s3Client, instanceProperties))
                .newInstanceValidator(new NewInstanceValidator(s3Client, dynamoClient));
    }

    /**
     * Reads configuration from the CDK context. Usually only used when deploying a single instance of Sleeper in its
     * own CDK app. Will read instance and table properties from the local file system.
     *
     * @param  scope        the scope to read context variables from
     * @param  s3Client     the S3 client, to scan for jars to deploy and validate the current state
     * @param  dynamoClient the DynamoDB client, to validate the current state
     * @return              the configuration
     */
    public static SleeperInstanceProps fromContext(Construct scope, S3Client s3Client, DynamoDbClient dynamoClient) {
        return fromContext(CdkContext.from(scope), s3Client, dynamoClient);
    }

    /**
     * Reads configuration from the CDK context. Usually only used when deploying a single instance of Sleeper in its
     * own CDK app. Will read instance and table properties from the local file system.
     *
     * @param  context      the context to read variables from
     * @param  s3Client     the S3 client, to scan for jars to deploy and validate the current state
     * @param  dynamoClient the DynamoDB client, to validate the current state
     * @return              the configuration
     */
    public static SleeperInstanceProps fromContext(CdkContext context, S3Client s3Client, DynamoDbClient dynamoClient) {
        Path propertiesFile = Path.of(context.tryGetContext("propertiesfile"));
        SleeperInstanceConfiguration configuration = SleeperInstanceConfiguration.fromLocalConfiguration(propertiesFile);
        String instanceId = context.tryGetContext("id");
        if (instanceId != null) {
            configuration.getInstanceProperties().set(ID, instanceId);
        }
        String artefactsId = context.tryGetContext("artefactsId");
        if (artefactsId != null) {
            configuration.getInstanceProperties().set(ARTEFACTS_DEPLOYMENT_ID, artefactsId);
        }
        return builder(configuration.getInstanceProperties(), s3Client, dynamoClient)
                .tableProperties(configuration.getTableProperties())
                .networkingProvider(scope -> SleeperNetworking.createByContext(scope, context, configuration))
                .validateProperties(context.getBooleanOrDefault("validate", true))
                .ensureInstanceDoesNotExist(context.getBooleanOrDefault("newinstance", false))
                .skipCheckingVersionMatchesProperties(context.getBooleanOrDefault("skipVersionCheck", false))
                .deployPaused(context.getBooleanOrDefault("deployPaused", false))
                .build();
    }

    public void prepareProperties(Stack stack, SleeperNetworking networking) {

        CdkDefinedInstanceProperty.getAll().forEach(instanceProperties::unset);
        instanceProperties.set(VERSION, version);
        instanceProperties.set(ACCOUNT, stack.getAccount());
        instanceProperties.set(REGION, stack.getRegion());
        instanceProperties.set(VPC_ID, networking.vpcId());
        instanceProperties.setList(SUBNETS, networking.subnetIds());
        validate();
    }

    public void validate() {
        if (validateProperties) {
            instanceProperties.validate();
            try {
                BucketUtils.isValidDnsBucketName(instanceProperties.get(ID), true);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Sleeper instance ID is not valid as part of an S3 bucket name: " + instanceProperties.get(ID),
                        e);
            }
            for (TableProperties table : tableProperties) {
                table.validate();
            }
        }
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public List<TableProperties> getTableProperties() {
        return tableProperties;
    }

    public SleeperJarVersionIdsCache getJars() {
        return jars;
    }

    public SleeperArtefacts getArtefacts() {
        return artefacts;
    }

    public SleeperLambdaCode lambdaCode(Construct scope) {
        return lambdaCode(createJarsBucketReference(scope, "LambdaCodeBucket"));
    }

    private SleeperLambdaCode lambdaCode(IBucket jarsBucket) {
        return SleeperLambdaCode.from(instanceProperties, artefacts, jarsBucket);
    }

    private IBucket createJarsBucketReference(Construct scope, String id) {
        return Bucket.fromBucketName(scope, id, instanceProperties.get(JARS_BUCKET));
    }

    public boolean isDeployPaused() {
        return deployPaused;
    }

    public SleeperNetworkingProvider getNetworkingProvider() {
        return networkingProvider;
    }

    public static class Builder {
        private InstanceProperties instanceProperties;
        private SleeperJarVersionIdsCache jars;
        private SleeperArtefacts artefacts;
        private NewInstanceValidator newInstanceValidator;
        private List<TableProperties> tableProperties = List.of();
        private SleeperNetworkingProvider networkingProvider = scope -> SleeperNetworking.createByProperties(scope, instanceProperties);
        private String version = SleeperVersion.getVersion();
        private boolean validateProperties = true;
        private boolean ensureInstanceDoesNotExist = false;
        private boolean skipCheckingVersionMatchesProperties = false;
        private boolean deployPaused = false;

        /**
         * Sets the properties of the Sleeper instance to deploy. This is required.
         *
         * @param  instanceProperties the instance properties
         * @return                    this builder
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Sets how to find jars to deploy lambda functions. This is required.
         * <p>
         * This will be used to find the latest version of each jar in a versioned S3 bucket. The deployment will be
         * done against a specific version of each jar. It will only check the bucket once for each jar, and you can
         * reuse the same object for multiple Sleeper instances.
         *
         * @param  jars the jars
         * @return      this builder
         */
        public Builder jars(SleeperJarVersionIdsCache jars) {
            this.jars = jars;
            return this;
        }

        /**
         * Sets how to find artefacts to deploy in the instance. This is required.
         * <p>
         * This will be used for jars to deploy to AWS Lambda, and for Docker images to deploy to ECS and others.
         *
         * @param  artefacts the artefacts
         * @return           this builder
         */
        public Builder artefacts(SleeperArtefacts artefacts) {
            this.artefacts = artefacts;
            return this;
        }

        /**
         * Sets how to validate that a new instance does not already exist. This is required if
         * {@link #ensureInstanceDoesNotExist(boolean)} is set to true.
         *
         * @param  newInstanceValidator the validator
         * @return                      this builder
         */
        public Builder newInstanceValidator(NewInstanceValidator newInstanceValidator) {
            this.newInstanceValidator = newInstanceValidator;
            return this;
        }

        /**
         * Sets the properties of the Sleeper tables that will exist in this instance. This will not add these tables to
         * the instance, but will be used for optional stacks that require knowledge of tables in the instance at
         * deployment time. For example, DashboardStack creates dashboard UI elements for each table.
         * <p>
         * Default: an empty list (e.g. tables will not be shown on the dashboard deployed by DashboardStack)
         *
         * @param  tableProperties the table properties
         * @return                 this builder
         */
        public Builder tableProperties(List<TableProperties> tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        /**
         * Sets the networking settings to deploy an instance of Sleeper.
         *
         * @param  networking the networking settings
         * @return            this builder
         */
        public Builder networking(SleeperNetworking networking) {
            return networkingProvider(scope -> networking);
        }

        /**
         * Sets the networking settings to deploy an instance of Sleeper.
         *
         * @param  networkingProvider a provider for the networking settings
         * @return                    this builder
         */
        public Builder networkingProvider(SleeperNetworkingProvider networkingProvider) {
            this.networkingProvider = networkingProvider;
            return this;
        }

        /**
         * Sets the version of Sleeper being deployed. Normally this should only be set in tests, in order to control
         * the output deterministically as the version of Sleeper changes. In all other circumstances it should be left
         * to default to the current version being used.
         *
         * @param  version the version number
         * @return         this builder
         */
        public Builder version(String version) {
            this.version = version;
            return this;
        }

        /**
         * Sets whether to validate the instance and table properties before deployment.
         * <p>
         * Default: true
         *
         * @param  validateProperties true if the properties should be validated
         * @return                    this builder
         */
        public Builder validateProperties(boolean validateProperties) {
            this.validateProperties = validateProperties;
            return this;
        }

        /**
         * Sets whether to check that the instance does not exist before deployment. Can be used when deploying a fresh
         * instance.
         * <p>
         * Default: false
         *
         * @param  ensureInstanceDoesNotExist true to check the instance does not exist
         * @return                            this builder
         */
        public Builder ensureInstanceDoesNotExist(boolean ensureInstanceDoesNotExist) {
            this.ensureInstanceDoesNotExist = ensureInstanceDoesNotExist;
            return this;
        }

        /**
         * Sets whether to check that the version in the instance properties matches the version of Sleeper being used
         * for the deployment. This check can be useful when the instance properties were loaded from an existing
         * instance, or when a specific version of Sleeper is set in your configuration. If no version is set in the
         * instance properties, this will be ignored and no check will be made.
         * <p>
         * Default: false
         *
         * @param  skipCheckingVersionMatchesProperties true to skip checking that the version number matches
         * @return                                      this builder
         */
        public Builder skipCheckingVersionMatchesProperties(boolean skipCheckingVersionMatchesProperties) {
            this.skipCheckingVersionMatchesProperties = skipCheckingVersionMatchesProperties;
            return this;
        }

        /**
         * Sets whether the instance should be paused or not. If this is true, schedules will be disabled that trigger
         * regular operations, or start tasks to run pending operations. An instance can also be paused or resumed
         * separately after deployment.
         *
         * @param  deployPaused true if the instance should be paused
         * @return              this builder
         */
        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public SleeperInstanceProps build() {
            Objects.requireNonNull(instanceProperties, "instanceProperties must not be null");
            Objects.requireNonNull(tableProperties, "tableProperties must not be null");
            Objects.requireNonNull(jars, "jars must not be null");

            Properties tagsProperties = instanceProperties.getTagsProperties();
            tagsProperties.setProperty("InstanceID", instanceProperties.get(ID));
            instanceProperties.loadTags(tagsProperties);

            if (ensureInstanceDoesNotExist) {
                newInstanceValidator.validate(instanceProperties);
            }

            String deployedVersion = instanceProperties.get(VERSION);
            if (!skipCheckingVersionMatchesProperties
                    && deployedVersion != null
                    && !version.equals(deployedVersion)) {
                throw new MismatchedVersionException(String.format("Local version %s does not match deployed version %s. " +
                        "Please upgrade/downgrade to make these match",
                        version, deployedVersion));
            }

            return new SleeperInstanceProps(this);
        }
    }

}
