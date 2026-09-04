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
package sleeper.systemtest.drivers.cdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.deploy.DeployInstance;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.deploy.DeployNewInstance.InstancePropertiesLoader;
import sleeper.clients.deploy.DeployNewInstance.StoreFactory;
import sleeper.clients.deploy.InstanceDeployer;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewTestInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewTestInstance.class);
    // No access modifier, so the test can reuse these instead of hard-coding the same strings again.
    static final String DEFAULT_CONFIG_DIRECTORY = "test/deployAll";
    static final String INSTANCE_PROPERTIES_FILE = "system-test-instance.properties";

    // Defines the positional arguments, the options that may be set, and the help text.
    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .systemArguments(List.of("scriptsDirectory"))
            .positionalArguments(List.of("scriptsDirectory", "instanceId", "vpcId", "subnetIds"))
            .options(List.of(
                    CommandOption.longOption("config-dir"),
                    CommandOption.longFlag("paused"),
                    CommandOption.longOption("properties-file")))
            .helpSummary(
                    """
                            Deploys a demonstration instance of Sleeper with system test properties.

                            If neither --properties-file nor --config-dir is set, the instance and a "system-test" table are deployed from the default system test configuration in scripts/test/deployAll. This is the default.

                            --config-dir <dir>
                            Path to a full configuration directory (instance.properties plus tables). The instance and its tables are deployed as-is. Cannot be combined with --properties-file.

                            --paused
                            If set, the instance will be deployed paused. Periodic background processes will not run until the instance is manually resumed.

                            --properties-file <file>
                            Path to an instance.properties file. Only the instance configuration is read; no tables are deployed. Cannot be combined with --config-dir.""")
            .build();

    private DeployNewTestInstance() {
    }

    /**
     * Reads a parsed command line into a typed set of arguments.
     *
     * @param  arguments the parsed command line
     * @return           the typed arguments
     */
    public static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(
                Path.of(arguments.getString("scriptsDirectory")),
                arguments.getString("instanceId"),
                arguments.getString("vpcId"),
                arguments.getString("subnetIds"),
                arguments.getOptionalString("properties-file").map(Path::of).orElse(null),
                arguments.getOptionalString("config-dir").map(Path::of).orElse(null),
                arguments.isFlagSet("paused"));
    }

    /**
     * Works out the instance and table configuration to deploy, based on which options were given.
     *
     * @param  args        the parsed command line arguments
     * @return             the configuration to deploy
     * @throws IOException if the default configuration files could not be created
     */
    public static SleeperInstanceConfiguration loadConfiguration(Arguments args) throws IOException {
        SleeperInstanceConfiguration config;
        if (args.propertiesFile() != null) {
            // Read only the instance configuration. Any tables must be given explicitly with --config-dir.
            LOGGER.info("Properties file specified, reading instance configuration only");
            config = SleeperInstanceConfiguration.fromLocalConfiguration(args.propertiesFile());
        } else if (args.configDir() != null) {
            // Load the instance and any tables defined in the directory.
            LOGGER.info("Configuration directory specified, reading its instance and tables");
            config = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(args.configDir());
        } else {
            // Default to the system test configuration held in the deployment directory.
            Path instancePropertiesFile = defaultInstancePropertiesFile(args);
            LOGGER.info("No configuration specified, using the system test configuration in {}", instancePropertiesFile.getParent());
            config = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(instancePropertiesFile);
        }
        config.getInstanceProperties().set(ID, args.instanceId());
        config.getInstanceProperties().set(VPC_ID, args.vpcId());
        config.getInstanceProperties().set(SUBNETS, args.subnetIds());
        return config;
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, DeployNewTestInstance::readArguments);

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
            PartitionMetadata partitionMetadata = PartitionMetadata.of(region);

            deploy(args,
                    DeployInstance.fromScriptsDirectory(args.scriptsDirectory(), accountName, region, partitionMetadata, s3Client, ecrClient),
                    StoreFactory.withAwsClients(s3Client, dynamoClient),
                    id -> S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, id));
        }
    }

    /**
     * Deploys the demonstration instance using the given deployment collaborators. The collaborators form the seam that
     * lets tests substitute in-memory fakes for the AWS-backed deployment.
     *
     * @param args           the parsed command line arguments
     * @param deployInstance deploys the instance and runs the CDK
     * @param storeFactory   creates the table and state stores
     * @param loader         loads the deployed instance properties
     */
    static void deploy(Arguments args, InstanceDeployer deployInstance, StoreFactory storeFactory, InstancePropertiesLoader loader)
            throws IOException, InterruptedException {
        SleeperInstanceConfiguration config = loadConfiguration(args);

        // Point the CDK at the config given on the command line, defaulting to the demo instance properties file.
        // The tables are created after the CDK runs, so it only needs the instance configuration.
        DeployNewInstance.builder()
                .deployInstance(deployInstance)
                .storeFactory(storeFactory)
                .instancePropertiesLoader(loader)
                .expectedInstanceConfiguration(config)
                .cdkApp(SleeperInternalCdkApp.DEMONSTRATION)
                .propertiesFile(resolvePropertiesFile(args))
                .configDir(args.configDir())
                .deployPaused(args.deployPaused())
                .build().deploy();
    }

    // The default demo configuration directory, resolved relative to the scripts directory.
    private static Path defaultConfigDir(Arguments args) {
        return args.scriptsDirectory().resolve(DEFAULT_CONFIG_DIRECTORY);
    }

    // Create each real config file from its template on first use; files that already exist are left as they are.
    private static void copyTemplatesIfMissing(Path configDir) throws IOException {
        for (String fileName : List.of(INSTANCE_PROPERTIES_FILE, "table.properties", "schema.json", "tags.properties")) {
            Path file = configDir.resolve(fileName);
            if (!Files.exists(file)) {
                Files.copy(configDir.resolve(fileName + ".template"), file);
            }
        }
    }

    // Decides which properties file to hand to the CDK: whatever was given on the command line, or the demo's own
    // instance properties file if neither --properties-file nor --config-dir was set.
    private static Path resolvePropertiesFile(Arguments args) throws IOException {
        if (args.propertiesFile() != null || args.configDir() != null) {
            return args.propertiesFile();
        }
        return defaultInstancePropertiesFile(args);
    }

    // The demo's own instance properties file, seeding it from its template first if it doesn't exist yet. Shared by
    // loadConfiguration and resolvePropertiesFile so the default can't drift between the two.
    private static Path defaultInstancePropertiesFile(Arguments args) throws IOException {
        Path configDir = defaultConfigDir(args);
        copyTemplatesIfMissing(configDir);
        return configDir.resolve(INSTANCE_PROPERTIES_FILE);
    }

    /**
     * The command line arguments after parsing. Only one of a properties file or a configuration directory may be set.
     *
     * @param scriptsDirectory the directory holding the deployment scripts and templates
     * @param instanceId       the ID to give the deployed instance
     * @param vpcId            the ID of the VPC to deploy into
     * @param subnetIds        the IDs of the subnets to deploy into
     * @param propertiesFile   an instance properties file to read the instance configuration from, or null
     * @param configDir        a full configuration directory to deploy, or null
     * @param deployPaused     true to deploy the instance paused
     */
    public record Arguments(
            Path scriptsDirectory,
            String instanceId,
            String vpcId,
            String subnetIds,
            Path propertiesFile,
            Path configDir,
            boolean deployPaused) {

        public Arguments {
            if (propertiesFile != null && configDir != null) {
                throw new CommandArgumentsException("Cannot use both --properties-file and --config-dir");
            }
        }
    }
}
