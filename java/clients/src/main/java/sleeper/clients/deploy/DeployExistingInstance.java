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

import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CDK_APP;

public class DeployExistingInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeployExistingInstance.class);
    private final Path scriptsDirectory;
    private final InstanceProperties properties;
    private final List<TableProperties> tablePropertiesList;
    private final S3Client s3;
    private final String accountName;
    private final AwsRegionProvider regionProvider;
    private final boolean deployPaused;
    private final SleeperInternalCdkApp forceCdkApp;
    private final CommandPipelineRunner runCommand;
    private final boolean createMultiPlatformBuilder;

    private DeployExistingInstance(Builder builder) {
        scriptsDirectory = builder.scriptsDirectory;
        properties = builder.properties;
        tablePropertiesList = builder.tablePropertiesList;
        s3 = builder.s3;
        accountName = builder.accountName;
        regionProvider = builder.regionProvider;
        deployPaused = builder.deployPaused;
        forceCdkApp = builder.forceCdkApp;
        runCommand = builder.runCommand;
        createMultiPlatformBuilder = builder.createMultiPlatformBuilder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        CommandLineUsage usage = CommandLineUsage.builder()
                .positionalArguments(List.of("scripts directory", "instance ID"))
                .systemArguments(List.of("scripts directory"))
                .options(List.of(CommandOption.longFlag("paused"), CommandOption.longOption("force-cdk-app")))
                .helpSummary("" +
                        "Redeploys an existing Sleeper instance. This can only be used with an instance that was deployed " +
                        "with the standard scripts or CDK app from the main Sleeper GitHub.\n" +
                        "\n" +
                        "--paused\n" +
                        "If set, the instance will be deployed paused. Periodic background processes will not run until " +
                        "the instance is manually resumed.\n" +
                        "\n" +
                        "--force-cdk-app <app>\n" +
                        "This can be used to force use of a specific CDK app to deploy the instance. Usually the CDK app " +
                        "will be automatically detected. This should only be used if the detection fails, for example if " +
                        "you are upgrading from a version that did not have this auto-detection. Do not use this if the " +
                        "instance was deployed with a CDK app that is not listed.\n" +
                        "Available apps from Sleeper GitHub: " + SleeperInternalCdkApp.describeCdkAppsDeployingSleeperInstance())
                .build();
        Arguments args = CommandArguments.parseAndValidateOrExit(usage, rawArgs, arguments -> new Arguments(
                Path.of(arguments.getString("scripts directory")),
                arguments.getString("instance ID"),
                arguments.isFlagSet("paused"),
                arguments.getOptionalString("force-cdk-app")
                        .flatMap(SleeperInternalCdkApp::readCdkAppDeployingSleeperInstance)
                        .orElse(null)));

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                EcrClient ecrClient = EcrClient.create();
                StsClient stsClient = StsClient.create()) {
            builder().clients(s3Client, stsClient)
                    .regionProvider(DefaultAwsRegionProviderChain.builder().build())
                    .scriptsDirectory(args.scriptsDirectory())
                    .instanceId(args.instanceId())
                    .deployPaused(args.deployPaused())
                    .forceCdkApp(args.forceCdkApp())
                    .loadPropertiesFromS3(s3Client, dynamoClient)
                    .build().update();
        }
    }

    public record Arguments(Path scriptsDirectory, String instanceId, boolean deployPaused, SleeperInternalCdkApp forceCdkApp) {
    }

    public void update() throws IOException, InterruptedException {
        DeployInstance deployInstance = new DeployInstance(
                SyncJars.fromScriptsDirectory(s3, accountName, scriptsDirectory),
                new UploadDockerImagesToEcr(
                        UploadDockerImages.builder()
                                .scriptsDirectory(scriptsDirectory)
                                .deployConfig(DeployConfiguration.fromScriptsDirectory(scriptsDirectory))
                                .commandRunner(runCommand)
                                .createMultiplatformBuilder(createMultiPlatformBuilder)
                                .build(),
                        accountName, regionProvider.getRegion().id()),
                DeployInstance.WriteLocalProperties.underScriptsDirectory(scriptsDirectory),
                InvokeCdk.builder().scriptsDirectory(scriptsDirectory).runCommand(runCommand).build());

        deployInstance.deploy(DeployInstanceRequest.builder()
                .instanceConfig(SleeperInstanceConfiguration.builder().instanceProperties(properties).tableProperties(tablePropertiesList).build())
                .cdkCommand(deployPaused ? CdkCommand.deployExistingPaused() : CdkCommand.deployExisting())
                .cdkApp(getCdkApp())
                .build());

        LOGGER.info("Finished deployment of existing instance");
    }

    private SleeperInternalCdkApp getCdkApp() {
        if (forceCdkApp != null) {
            return forceCdkApp;
        }
        return properties.getOptionalEnumValue(CDK_APP, SleeperInternalCdkApp.class)
                .orElseThrow(() -> new IllegalArgumentException("" +
                        "Cannot find the CDK app used to deploy this instance. " +
                        "This script can only be used if you deployed with a CDK app that is included in the main Sleeper GitHub. " +
                        "If you did, this failure may happen when upgrading from a version that did not have autodetection of the CDK app. " +
                        "You can force which CDK app to use with the --force-cdk-app command line option. Use --help for more details."));
    }

    public static final class Builder {
        private Path scriptsDirectory;
        private String instanceId;
        private InstanceProperties properties;
        private List<TableProperties> tablePropertiesList;
        private String accountName;
        private S3Client s3;
        private AwsRegionProvider regionProvider;
        private boolean deployPaused;
        private SleeperInternalCdkApp forceCdkApp;
        private CommandPipelineRunner runCommand = CommandUtils::runCommandInheritIO;
        private boolean createMultiPlatformBuilder = true;

        private Builder() {
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder properties(InstanceProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder tableProperties(TableProperties... tableProperties) {
            return tablePropertiesList(List.of(tableProperties));
        }

        public Builder tablePropertiesList(List<TableProperties> tablePropertiesList) {
            this.tablePropertiesList = tablePropertiesList;
            return this;
        }

        public Builder clients(S3Client s3, StsClient sts) {
            this.s3 = s3;
            this.accountName = sts.getCallerIdentity().account();
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public Builder forceCdkApp(SleeperInternalCdkApp forceCdkApp) {
            this.forceCdkApp = forceCdkApp;
            return this;
        }

        public Builder runCommand(CommandPipelineRunner runCommand) {
            this.runCommand = runCommand;
            return this;
        }

        public Builder createMultiPlatformBuilder(boolean createMultiPlatformBuilder) {
            this.createMultiPlatformBuilder = createMultiPlatformBuilder;
            return this;
        }

        public Builder loadPropertiesFromS3(S3Client s3Client, DynamoDbClient dynamoCient) {
            properties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
            tablePropertiesList = S3TableProperties.createStore(properties, s3Client, dynamoCient)
                    .streamAllTables().collect(Collectors.toList());
            return this;
        }

        public DeployExistingInstance build() {
            return new DeployExistingInstance(this);
        }
    }
}
