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
package sleeper.systemtest.drivers.cdk;

import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.deploy.container.StackDockerImage;
import sleeper.clients.deploy.properties.PopulateInstancePropertiesAws;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.deploy.DeployInstanceConfigurationFromTemplates;
import sleeper.core.deploy.PopulateInstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static sleeper.clients.deploy.container.StackDockerImage.dockerBuildImage;
import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_REPO;

public class DeployNewTestInstance {

    private DeployNewTestInstance() {
    }

    public static final StackDockerImage SYSTEM_TEST_IMAGE = dockerBuildImage("system-test");

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 5 || args.length > 7) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <properties-template> <instance-id> <vpc> <csv-list-of-subnets> " +
                    "<optional-deploy-paused-flag> <optional-split-points-file>");
        }
        AwsRegionProvider regionProvider = DefaultAwsRegionProviderChain.builder().build();
        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            Path scriptsDirectory = Path.of(args[0]);
            Path propertiesFile = Path.of(args[1]);
            PopulateInstanceProperties populateInstanceProperties = PopulateInstancePropertiesAws.builder(stsClient, regionProvider)
                    .instanceId(args[2]).vpcId(args[3]).subnetIds(args[4])
                    .extraInstanceProperties(properties -> properties.set(SYSTEM_TEST_REPO, args[2] + "/system-test"))
                    .build();
            boolean deployPaused = "true".equalsIgnoreCase(optionalArgument(args, 5).orElse("false"));
            Path splitPointsFileForTemplate = optionalArgument(args, 6).map(Path::of).orElse(null);
            DeployNewInstance.builder().scriptsDirectory(scriptsDirectory)
                    .deployInstanceConfiguration(DeployInstanceConfiguration.forNewInstanceDefaultingTables(
                            propertiesFile, populateInstanceProperties,
                            templates(scriptsDirectory, splitPointsFileForTemplate)))
                    .extraDockerImages(List.of(SYSTEM_TEST_IMAGE))
                    .deployPaused(deployPaused)
                    .instanceType(InvokeCdkForInstance.Type.SYSTEM_TEST)
                    .deployWithClients(s3Client, dynamoClient, ecrClient);
        }
    }

    private static DeployInstanceConfigurationFromTemplates templates(Path scriptsDir, Path splitPointsFileForTemplate) {
        return DeployInstanceConfigurationFromTemplates.builder()
                .templatesDir(scriptsDir.resolve("templates"))
                .tableNameForTemplate("system-test")
                .splitPointsFileForTemplate(splitPointsFileForTemplate)
                .build();
    }
}
