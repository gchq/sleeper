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

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.internal.BucketUtils;
import software.constructs.Construct;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.util.CdkContext;
import sleeper.cdk.util.MismatchedVersionException;
import sleeper.cdk.util.NewInstanceValidator;
import sleeper.core.SleeperVersion;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Configuration to deploy a Sleeper instance with the CDK.
 */
public class SleeperInstanceProps {

    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tableProperties;
    private final SleeperJarsInBucket jars;
    private final boolean deployPaused;

    private SleeperInstanceProps(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
        jars = builder.jars;
        deployPaused = builder.deployPaused;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoClient) {
        return builder()
                .instanceProperties(instanceProperties)
                .jars(SleeperJarsInBucket.from(s3Client, instanceProperties))
                .newInstanceValidator(new NewInstanceValidator(s3Client, dynamoClient));
    }

    public static SleeperInstanceProps fromContext(Construct scope, S3Client s3Client, DynamoDbClient dynamoClient) {
        return fromContext(CdkContext.from(scope), s3Client, dynamoClient);
    }

    public static SleeperInstanceProps fromContext(CdkContext context, S3Client s3Client, DynamoDbClient dynamoClient) {
        Path propertiesFile = Path.of(context.tryGetContext("propertiesfile"));
        DeployInstanceConfiguration configuration = DeployInstanceConfiguration.fromLocalConfiguration(propertiesFile);
        return builder(configuration.getInstanceProperties(), s3Client, dynamoClient)
                .tableProperties(configuration.getTableProperties())
                .validateProperties(!"false".equalsIgnoreCase(context.tryGetContext("validate")))
                .ensureInstanceDoesNotExist("true".equalsIgnoreCase(context.tryGetContext("newinstance")))
                .checkVersionMatchesProperties(!"true".equalsIgnoreCase(context.tryGetContext("skipVersionCheck")))
                .deployPaused("true".equalsIgnoreCase(context.tryGetContext("deployPaused")))
                .build();
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public List<TableProperties> getTableProperties() {
        return tableProperties;
    }

    public SleeperJarsInBucket getJars() {
        return jars;
    }

    public boolean isDeployPaused() {
        return deployPaused;
    }

    public static class Builder {
        private InstanceProperties instanceProperties;
        private List<TableProperties> tableProperties = List.of();
        private SleeperJarsInBucket jars;
        private NewInstanceValidator newInstanceValidator;
        private boolean validateProperties = true;
        private boolean ensureInstanceDoesNotExist = false;
        private boolean checkVersionMatchesProperties = true;
        private boolean deployPaused = false;

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableProperties(List<TableProperties> tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder jars(SleeperJarsInBucket jars) {
            this.jars = jars;
            return this;
        }

        public Builder newInstanceValidator(NewInstanceValidator newInstanceValidator) {
            this.newInstanceValidator = newInstanceValidator;
            return this;
        }

        public Builder validateProperties(boolean validateProperties) {
            this.validateProperties = validateProperties;
            return this;
        }

        public Builder ensureInstanceDoesNotExist(boolean ensureInstanceDoesNotExist) {
            this.ensureInstanceDoesNotExist = ensureInstanceDoesNotExist;
            return this;
        }

        public Builder checkVersionMatchesProperties(boolean checkVersionMatchesProperties) {
            this.checkVersionMatchesProperties = checkVersionMatchesProperties;
            return this;
        }

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public SleeperInstanceProps build() {
            Objects.requireNonNull(instanceProperties, "instanceProperties must not be null");
            Objects.requireNonNull(tableProperties, "tableProperties must not be null");
            Objects.requireNonNull(jars, "jars must not be null");

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

            if (ensureInstanceDoesNotExist) {
                newInstanceValidator.validate(instanceProperties);
            }

            String deployedVersion = instanceProperties.get(VERSION);
            String localVersion = SleeperVersion.getVersion();
            if (checkVersionMatchesProperties
                    && deployedVersion != null
                    && !localVersion.equals(deployedVersion)) {
                throw new MismatchedVersionException(String.format("Local version %s does not match deployed version %s. " +
                        "Please upgrade/downgrade to make these match",
                        localVersion, deployedVersion));
            }

            CdkDefinedInstanceProperty.getAll().forEach(instanceProperties::unset);
            instanceProperties.set(VERSION, localVersion);

            return new SleeperInstanceProps(this);
        }
    }

}
