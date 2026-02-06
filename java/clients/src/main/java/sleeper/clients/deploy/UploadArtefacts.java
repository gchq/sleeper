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
import sleeper.clients.deploy.container.StackDockerImage;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrRequest;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.model.SleeperArtefactsLocation;
import sleeper.core.properties.model.SleeperPropertyValueUtils;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ARTEFACTS_DEPLOYMENT_ID;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

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
                        CommandOption.longOption("extra-images"),
                        CommandOption.longFlag("create-builder"),
                        CommandOption.longFlag("create-deployment")))
                .helpSummary("Uploads jars and Docker images to AWS. You must set either an instance properties file " +
                        "or an artefacts deployment ID to upload to.\n" +
                        "\n" +
                        "This works against an artefacts CDK deployment that must already exist in the same AWS " +
                        "account that you want to deploy Sleeper to. If you use the scripts for deployment you will " +
                        "not need this, as this is done as part of \"deploy.sh\". If you prefer to use the artefacts " +
                        "CDK app directly, you can then use this tool to upload the needed artefacts to that " +
                        "deployment.\n" +
                        "\n" +
                        "--properties, -p\n" +
                        "An instance properties file to read configuration from. If you do not also set the " +
                        "artefacts deployment ID, it will be read from this file, defaulting to the instance ID. " +
                        "Docker images that are not required to deploy this instance will not be uploaded.\n" +
                        "\n" +
                        "--id, -i\n" +
                        "An artefacts deployment ID to upload to. All Docker images will be uploaded.\n" +
                        "\n" +
                        "--extra-images\n" +
                        "A comma-separated list of extra Docker images to upload. We will assume these are in the\n" +
                        "same location as the other Docker images, and are not multiplatform." +
                        "\n" +
                        "--create-builder\n" +
                        "By default, a Docker builder will be created suitable for multiplatform builds, with " +
                        "\"docker buildx create --name sleeper --use\". If you set up a suitable builder yourself " +
                        "instead, you can use --create-builder=false to turn off this behaviour.\n" +
                        "\n" +
                        "--create-deployment\n" +
                        "By default, we assume you have deployed an artefacts deployment separately. If you set this " +
                        "flag, this tool will deploy a new artefacts CDK deployment for you.")
                .build();
        Arguments args = CommandArguments.parseAndValidateOrExit(usage, rawArgs, arguments -> new Arguments(
                Path.of(arguments.getString("scripts directory")),
                arguments.getOptionalString("properties")
                        .map(Path::of)
                        .map(LoadLocalProperties::loadInstancePropertiesNoValidation)
                        .orElse(null),
                arguments.getOptionalString("id")
                        .orElse(null),
                arguments.getOptionalString("extra-images")
                        .map(string -> SleeperPropertyValueUtils.readList(string).stream()
                                .map(StackDockerImage::dockerBuildImage).toList())
                        .orElse(List.of()),
                arguments.isFlagSetWithDefault("create-builder", true),
                arguments.isFlagSetWithDefault("create-deployment", false)));

        String deploymentId;
        String jarsBucket;
        String ecrPrefix;
        List<StackDockerImage> images;
        if (args.instanceProperties() != null) {
            if (args.deploymentId() != null) {
                args.instanceProperties().set(ARTEFACTS_DEPLOYMENT_ID, args.deploymentId());
            }
            deploymentId = args.instanceProperties().get(ARTEFACTS_DEPLOYMENT_ID);
            jarsBucket = args.instanceProperties().get(JARS_BUCKET);
            ecrPrefix = args.instanceProperties().get(ECR_REPOSITORY_PREFIX);
            images = DockerImageConfiguration.getDefault().getImagesToUpload(args.instanceProperties());
        } else {
            deploymentId = args.deploymentId();
            jarsBucket = SleeperArtefactsLocation.getDefaultJarsBucketName(args.deploymentId());
            ecrPrefix = SleeperArtefactsLocation.getDefaultEcrRepositoryPrefix(args.deploymentId());
            images = DockerImageConfiguration.getDefault().getAllImagesToUpload();
        }

        try (S3Client s3Client = S3Client.create();
                EcrClient ecrClient = EcrClient.create();
                StsClient stsClient = StsClient.create()) {

            String account = stsClient.getCallerIdentity().account();
            String region = DefaultAwsRegionProviderChain.builder().build().getRegion().id();
            SyncJars syncJars = SyncJars.fromScriptsDirectory(s3Client, args.scriptsDir());
            UploadDockerImagesToEcr uploadImages = new UploadDockerImagesToEcr(
                    UploadDockerImages.builder()
                            .scriptsDirectory(args.scriptsDir())
                            .deployConfig(DeployConfiguration.fromScriptsDirectory(args.scriptsDir()))
                            .createMultiplatformBuilder(args.createMultiplatformBuilder())
                            .build(),
                    CheckVersionExistsInEcr.withEcrClient(ecrClient), account, region);

            if (args.createDeployment()) {
                InvokeCdk.fromScriptsDirectory(args.scriptsDir())
                        .invoke(InvokeCdk.Type.ARTEFACTS, CdkCommand.deployArtefacts(deploymentId, List.of()));
            }
            syncJars.sync(SyncJarsRequest.builder()
                    .bucketName(jarsBucket)
                    .build());
            uploadImages.upload(UploadDockerImagesToEcrRequest.builder()
                    .ecrPrefix(ecrPrefix)
                    .images(images)
                    .build().withExtraImages(args.extraImages()));
        }
    }

    public record Arguments(
            Path scriptsDir, InstanceProperties instanceProperties, String deploymentId,
            List<StackDockerImage> extraImages, boolean createMultiplatformBuilder, boolean createDeployment) {

        public Arguments {
            if (instanceProperties == null && deploymentId == null) {
                throw new CommandArgumentsException("Expected instance properties or artefacts deployment ID");
            }
        }
    }

}
