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
package sleeper.clients.deploy;

import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.deploy.container.CheckVersionExistsInEcr;
import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrRequest;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.model.SleeperArtefactsLocation;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Uploads jars and Docker images to AWS. The S3 jars bucket and the ECR repositories must already have been created,
 * e.g. by deploying SleeperArtefactsCdkApp.
 */
public class UploadArtefacts {

    private UploadArtefacts() {
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        CommandLineUsage usage = CommandLineUsage.builder()
                .systemArguments(List.of("scripts directory"))
                .options(List.of(
                        CommandOption.shortOption('p', "properties"),
                        CommandOption.shortOption('i', "id"),
                        CommandOption.longFlag("create-builder")))
                .helpSummary("Uploads jars and Docker images to AWS.\n" +
                        "\n" +
                        "If you set an instance.properties file with --properties, Docker images that are not " +
                        "required to deploy that instance will not be uploaded.\n" +
                        "\n" +
                        "If you set an instance ID or artefacts deployment ID with --id, all images will be " +
                        "uploaded.\n" +
                        "\n" +
                        "By default, a Docker builder will be created suitable for multiplatform builds, with " +
                        "\"docker buildx create --name sleeper --use\". If you set up a suitable builder yourself " +
                        "instead, you can use --create-builder=false to turn off this behaviour.")
                .build();
        Arguments args = CommandArguments.parseAndValidateOrExit(usage, rawArgs, arguments -> new Arguments(
                Path.of(arguments.getString("scripts directory")),
                arguments.getOptionalString("properties")
                        .map(Path::of)
                        .map(LoadLocalProperties::loadInstanceProperties)
                        .orElse(null),
                arguments.getOptionalString("id")
                        .orElse(null),
                arguments.isFlagSet("create-builder")));

        try (S3Client s3Client = S3Client.create();
                EcrClient ecrClient = EcrClient.create();
                StsClient stsClient = StsClient.create()) {
            SyncJars syncJars = SyncJars.fromScriptsDirectory(s3Client, args.scriptsDir());
            UploadDockerImagesToEcr uploadImages = new UploadDockerImagesToEcr(
                    UploadDockerImages.builder()
                            .scriptsDirectory(args.scriptsDir())
                            .deployConfig(DeployConfiguration.fromScriptsDirectory(args.scriptsDir()))
                            .createMultiplatformBuilder(args.createMultiplatformBuilder())
                            .build(),
                    CheckVersionExistsInEcr.withEcrClient(ecrClient));

            if (args.instanceProperties() != null) {
                syncJars.sync(SyncJarsRequest.from(args.instanceProperties()));
                uploadImages.upload(UploadDockerImagesToEcrRequest.forDeployment(args.instanceProperties()));
            } else {
                String account = stsClient.getCallerIdentity().account();
                String region = DefaultAwsRegionProviderChain.builder().build().getRegion().id();
                syncJars.sync(SyncJarsRequest.builder()
                        .bucketName(SleeperArtefactsLocation.getDefaultJarsBucketName(args.deploymentId()))
                        .region(region)
                        .build());
                uploadImages.upload(UploadDockerImagesToEcrRequest.builder()
                        .ecrPrefix(SleeperArtefactsLocation.getDefaultEcrRepositoryPrefix(args.deploymentId()))
                        .account(account)
                        .region(region)
                        .images(DockerImageConfiguration.getDefault().getAllImagesToUpload())
                        .build());
            }
        }
    }

    public record Arguments(Path scriptsDir, InstanceProperties instanceProperties, String deploymentId, boolean createMultiplatformBuilder) {

        public Arguments {
            if (instanceProperties == null && deploymentId == null) {
                throw new IllegalArgumentException("Expected instance properties or artefacts deployment ID");
            }
        }
    }

}
