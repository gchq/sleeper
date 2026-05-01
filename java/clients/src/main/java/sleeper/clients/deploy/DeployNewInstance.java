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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.deploy.container.StackDockerImage;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.table.AddTable;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final DeployInstance deployInstance;
    private final String accountName;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final SleeperInstanceConfiguration deployInstanceConfiguration;
    private final List<StackDockerImage> extraDockerImages;
    private final SleeperInternalCdkApp cdkApp;
    private final boolean deployPaused;

    private DeployNewInstance(Builder builder) {
        deployInstance = builder.deployInstance;
        accountName = builder.accountName;
        s3Client = builder.s3Client;
        dynamoClient = builder.dynamoClient;
        deployInstanceConfiguration = builder.deployInstanceConfiguration;
        extraDockerImages = builder.extraDockerImages;
        cdkApp = builder.cdkApp;
        deployPaused = builder.deployPaused;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4 || args.length > 6) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id> <vpc> <csv-list-of-subnets> " +
                    "<optional-instance-properties-file> <optional-deploy-paused-flag>");
        }
        Path scriptsDirectory = Path.of(args[0]);
        String instanceId = args[1];
        String vpcId = args[2];
        String subnetIds = args[3];
        Path instancePropertiesFile = optionalArgument(args, 4).map(Path::of).orElse(null);
        boolean deployPaused = "true".equalsIgnoreCase(optionalArgument(args, 5).orElse("false"));
        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            AwsRegionProvider regionProvider = DefaultAwsRegionProviderChain.builder().build();

            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.forNewInstanceDefaultingInstance(
                    instancePropertiesFile, scriptsDirectory.resolve("templates"));

            config.getInstanceProperties().set(ID, instanceId);
            config.getInstanceProperties().set(VPC_ID, vpcId);
            config.getInstanceProperties().set(SUBNETS, subnetIds);

            DeployInstance deployInstance = new DeployInstance(
                    SyncJars.fromScriptsDirectory(s3Client, accountName, scriptsDirectory),
                    new UploadDockerImagesToEcr(
                            UploadDockerImages.fromScriptsDirectory(scriptsDirectory, ecrClient),
                            accountName, regionProvider.getRegion().id()),
                    DeployInstance.WriteLocalProperties.underScriptsDirectory(scriptsDirectory),
                    InvokeCdk.fromScriptsDirectory(scriptsDirectory));
            builder().deployInstance(deployInstance)
                    .accountName(accountName)
                    .s3Client(s3Client)
                    .dynamoClient(dynamoClient)
                    .deployInstanceConfiguration(config)
                    .deployPaused(deployPaused)
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .build().deploy();
        }
    }

    public void deploy() throws IOException, InterruptedException {
        deployInstanceConfiguration.validate();

        deployInstance.deploy(DeployInstanceRequest.builder()
                .instanceConfig(deployInstanceConfiguration)
                .cdkCommand(deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew())
                .extraDockerImages(extraDockerImages)
                .cdkApp(cdkApp)
                .build());

        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, deployInstanceConfiguration.getInstanceId());
        for (TableProperties tableProperties : deployInstanceConfiguration.getTableProperties()) {
            LOGGER.info("Adding table " + tableProperties.getStatus());
            new AddTable(instanceProperties, tableProperties,
                    S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient),
                    StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                    .run();
        }
        LOGGER.info("Finished deployment of new instance");
    }

    public static final class Builder {
        private DeployInstance deployInstance;
        private String accountName;
        private S3Client s3Client;
        private DynamoDbClient dynamoClient;
        private SleeperInstanceConfiguration deployInstanceConfiguration;
        private List<StackDockerImage> extraDockerImages = List.of();
        private SleeperInternalCdkApp cdkApp;
        private boolean deployPaused;

        private Builder() {
        }

        public Builder deployInstance(DeployInstance deployInstance) {
            this.deployInstance = deployInstance;
            return this;
        }

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        public Builder deployInstanceConfiguration(SleeperInstanceConfiguration deployInstanceConfiguration) {
            this.deployInstanceConfiguration = deployInstanceConfiguration;
            return this;
        }

        public Builder extraDockerImages(List<StackDockerImage> extraDockerImages) {
            this.extraDockerImages = extraDockerImages;
            return this;
        }

        public Builder cdkApp(SleeperInternalCdkApp cdkApp) {
            this.cdkApp = cdkApp;
            return this;
        }

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public DeployNewInstance build() {
            return new DeployNewInstance(this);
        }

        public void deployWithClients(S3Client s3Client, DynamoDbClient dynamoClient) throws IOException, InterruptedException {
            s3Client(s3Client)
                    .dynamoClient(dynamoClient)
                    .build().deploy();
        }
    }
}
