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

import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.deploy.DeployInstance;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.deploy.DeployNewInstance.StoreFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.deploy.SleeperInstanceConfigurationFromTemplates;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.util.FilesUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewTestInstance {

    private DeployNewTestInstance() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 5 || args.length > 7) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <properties-template> <instance-id> <vpc> <csv-list-of-subnets> " +
                    "<optional-deploy-paused-flag> <optional-split-points-file>");
        }
        Path scriptsDirectory = Path.of(args[0]);
        Path configurationPath = Path.of(args[1]);
        String instanceId = args[2];
        String vpcId = args[3];
        String subnetIds = args[4];
        boolean deployPaused = "true".equalsIgnoreCase(optionalArgument(args, 5).orElse("false"));
        Path splitPointsFileForTemplate = optionalArgument(args, 6).map(Path::of).orElse(null);

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
            PartitionMetadata partitionMetadata = PartitionMetadata.of(region);

            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.forNewInstanceDefaultingTables(
                    configurationPath, templates(scriptsDirectory, splitPointsFileForTemplate));
            config.getInstanceProperties().set(ID, instanceId);
            config.getInstanceProperties().set(VPC_ID, vpcId);
            config.getInstanceProperties().set(SUBNETS, subnetIds);

            Path generatedDir = scriptsDirectory.resolve("generated");
            Files.createDirectories(generatedDir);
            FilesUtil.clearDirectory(generatedDir);
            SaveLocalProperties.saveToDirectory(generatedDir,
                    config.getInstanceProperties(),
                    config.getTableProperties().stream());

            DeployNewInstance.builder()
                    .deployInstance(DeployInstance.fromScriptsDirectory(scriptsDirectory, accountName, region, partitionMetadata, s3Client, ecrClient))
                    .storeFactory(StoreFactory.withAwsClients(s3Client, dynamoClient))
                    .instancePropertiesLoader(id -> S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, id))
                    .expectedInstanceConfiguration(config)
                    .cdkApp(SleeperInternalCdkApp.DEMONSTRATION)
                    .configDir(configurationPath)
                    .deployPaused(deployPaused)
                    .build().deploy();
        }
    }

    private static SleeperInstanceConfigurationFromTemplates templates(Path scriptsDir, Path splitPointsFileForTemplate) {
        return SleeperInstanceConfigurationFromTemplates.builder()
                .templatesDir(scriptsDir.resolve("templates"))
                .tableNameForTemplate("system-test")
                .splitPointsFileForTemplate(splitPointsFileForTemplate)
                .build();
    }
}
