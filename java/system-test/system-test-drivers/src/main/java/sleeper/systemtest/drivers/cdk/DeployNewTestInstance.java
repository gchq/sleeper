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
package sleeper.systemtest.drivers.cdk;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.deploy.PopulateInstancePropertiesAws;
import sleeper.clients.deploy.StackDockerImage;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.deploy.DeployInstanceConfigurationFromTemplates;
import sleeper.core.deploy.PopulateInstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;

public class DeployNewTestInstance {

    private DeployNewTestInstance() {
    }

    public static final StackDockerImage SYSTEM_TEST_IMAGE = dockerBuildImage("system-test");

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 5 || args.length > 7) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <properties-template> <instance-id> <vpc> <csv-list-of-subnets> " +
                    "<optional-deploy-paused-flag> <optional-split-points-file>");
        }
        AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        AwsRegionProvider regionProvider = DefaultAwsRegionProviderChain.builder().build();
        try (S3Client s3v2 = S3Client.create();
                EcrClient ecr = EcrClient.create()) {
            Path scriptsDirectory = Path.of(args[0]);
            Path propertiesFile = Path.of(args[1]);
            PopulateInstanceProperties populateInstanceProperties = PopulateInstancePropertiesAws.builder(sts, regionProvider)
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
                    .deployWithClients(s3, s3v2, dynamoDB, ecr);
        } finally {
            sts.shutdown();
            s3.shutdown();
            dynamoDB.shutdown();
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
