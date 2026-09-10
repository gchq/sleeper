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

import org.apache.commons.lang3.EnumUtils;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.deploy.container.BaseImageDestination;
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
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

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

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .systemArguments(List.of("scripts directory"))
            .options(List.of(
                    CommandOption.shortOption('p', "properties"),
                    CommandOption.shortOption('i', "id"),
                    CommandOption.longFlag("create-builder"),
                    CommandOption.longOption("base-image-registry"),
                    CommandOption.longFlag("create-deployment"),
                    CommandOption.shortOption('u', "upload"),
                    CommandOption.longOption("cdk-app")))
            .helpSummary("Uploads jars and Docker images to AWS. You must set either an instance properties file " +
                    "or an artefacts deployment ID to upload to.\n" +
                    "\n" +
                    "This works against an artefacts CDK deployment that must already exist in the same AWS " +
                    "account that you want to deploy Sleeper to. If you use the scripts for deployment you will " +
                    "not need this, as this is done as part of \"deployNew.sh\" or \"deployExisting.sh\". If you prefer to use the artefacts " +
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
                    "--create-builder\n" +
                    "By default, if you're uploading from a local build, a Docker builder will be created " +
                    "suitable for multiplatform builds. This will not be used when retrieving images from a " +
                    "remote repository. If you set up a suitable builder yourself instead, you can use " +
                    "--create-builder=false to turn off this behaviour.\n" +
                    "\n" +
                    "--base-image-registry <registry-prefix>\n" +
                    "By default, if you're uploading from a local build, a local Docker registry will be created " +
                    "to hold base images for further builds. This will not be used when retrieving images from a " +
                    "remote repository. If you set up a suitable registry yourself, you can use " +
                    "--base-image-registry <registry-prefix> to use that instead.\n" +
                    "\n" +
                    "--create-deployment\n" +
                    "By default, we assume you have deployed an artefacts deployment separately. If you set this " +
                    "flag, this tool will deploy a new artefacts CDK deployment for you.\n" +
                    "\n" +
                    "--upload, -u\n" +
                    "By default, all artefacts are uploaded. You can use \"--upload jars\" to only upload the " +
                    "jars, or \"--upload images\" to only upload the container images.\n" +
                    "\n" +
                    "--cdk-app\n" +
                    "By default we include images required for a normal Sleeper instance deployment. Other " +
                    "deployment types may need different Docker images, in which case you can set this to a CDK " +
                    "app that requires extra images.\n" +
                    "Valid values: " + SleeperInternalCdkApp.describeCdkAppsDeployingSleeperInstance())
            .build();

    public static Arguments readArguments(CommandArguments arguments, Function<Path, InstanceProperties> loadInstanceProperties) {
        return new Arguments(
                Path.of(arguments.getString("scripts directory")),
                arguments.getOptionalString("properties")
                        .map(Path::of)
                        .map(loadInstanceProperties)
                        .orElse(null),
                arguments.getOptionalString("id")
                        .orElse(null),
                arguments.getOptionalString("cdk-app")
                        .map(string -> SleeperInternalCdkApp.readCdkAppDeployingSleeperInstance(string)
                                .orElseThrow(() -> new CommandArgumentsException(
                                        "Unknown CDK app: " + string + ". Valid values: " +
                                                SleeperInternalCdkApp.describeCdkAppsDeployingSleeperInstance())))
                        .orElse(SleeperInternalCdkApp.STANDARD),
                arguments.isFlagSetWithDefault("create-builder", true),
                arguments.getOptionalString("base-image-registry")
                        .map(BaseImageDestination::fixedRegistry)
                        .orElseGet(() -> BaseImageDestination.managedRegistry(UploadDockerImages.BASE_IMAGE_REGISTRY_PORT)),
                arguments.isFlagSetWithDefault("create-deployment", false),
                arguments.getOptionalString("upload").map(ToUpload::fromString).orElse(ToUpload.ALL));
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, arguments -> readArguments(arguments, LoadLocalProperties::loadInstancePropertiesNoValidation));

        try (S3Client s3Client = S3Client.create();
                EcrClient ecrClient = EcrClient.create();
                StsClient stsClient = StsClient.create()) {

            String accountName = stsClient.getCallerIdentity().account();
            Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
            PartitionMetadata partitionMetadata = PartitionMetadata.of(region);

            InvokeCdk invokeCdk = InvokeCdk.fromScriptsDirectory(args.scriptsDir());
            SyncJars syncJars = SyncJars.fromScriptsDirectory(s3Client, accountName, args.scriptsDir());
            UploadDockerImagesToEcr uploadImages = new UploadDockerImagesToEcr(
                    UploadDockerImages.builderWith(args.scriptsDir(), ecrClient)
                            .createMultiplatformBuilder(args.createMultiplatformBuilder())
                            .baseImageDestination(args.baseImageDestination())
                            .build(),
                    accountName, region, partitionMetadata);

            upload(args, DockerImageConfiguration.getDefault(), new AwsClient(invokeCdk, syncJars, uploadImages));
        }
    }

    public static void upload(Arguments args, DockerImageConfiguration dockerImageConfiguration, Client client) throws IOException, InterruptedException {
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
            images = dockerImageConfiguration.getImagesToUpload(args.instanceProperties(), args.cdkApp());
        } else {
            deploymentId = args.deploymentId();
            jarsBucket = null;
            ecrPrefix = SleeperArtefactsLocation.getDefaultEcrRepositoryPrefix(args.deploymentId());
            images = dockerImageConfiguration.getAllImagesToUpload();
        }
        if (args.createDeployment()) {
            client.deployArtefactRepositories(deploymentId);
        }
        if (args.toUpload().isUploadJars()) {
            client.uploadJars(SyncJarsRequest.builder()
                    .bucketName(jarsBucket)
                    .deploymentId(deploymentId)
                    .build());
        }
        if (args.toUpload().isUploadImages()) {
            client.uploadImages(UploadDockerImagesToEcrRequest.builder()
                    .ecrPrefix(ecrPrefix)
                    .images(images)
                    .build());
        }
    }

    public record Arguments(
            Path scriptsDir,
            InstanceProperties instanceProperties,
            String deploymentId,
            SleeperInternalCdkApp cdkApp,
            boolean createMultiplatformBuilder,
            BaseImageDestination baseImageDestination,
            boolean createDeployment,
            ToUpload toUpload) {

        public Arguments {
            if (instanceProperties == null && deploymentId == null) {
                throw new CommandArgumentsException("Expected instance properties or artefacts deployment ID");
            }
        }
    }

    public enum ToUpload {
        ALL, JARS, IMAGES;

        public static ToUpload fromString(String string) {
            ToUpload upload = EnumUtils.getEnumIgnoreCase(ToUpload.class, string);
            if (upload == null) {
                throw new IllegalArgumentException("Unknown identifier for artefacts to upload: " + string);
            }
            return upload;
        }

        public boolean isUploadJars() {
            return this == ALL || this == JARS;
        }

        public boolean isUploadImages() {
            return this == ALL || this == IMAGES;
        }
    }

    /**
     * A client to upload artefacts to the deployment target.
     */
    public interface Client {

        void deployArtefactRepositories(String deploymentId) throws IOException, InterruptedException;

        void uploadJars(SyncJarsRequest request) throws IOException;

        void uploadImages(UploadDockerImagesToEcrRequest request) throws IOException, InterruptedException;
    }

    /**
     * A client to interact with AWS to upload artefacts.
     */
    public static class AwsClient implements Client {

        private final InvokeCdk invokeCdk;
        private final SyncJars syncJars;
        private final UploadDockerImagesToEcr uploadImages;

        public AwsClient(InvokeCdk invokeCdk, SyncJars syncJars, UploadDockerImagesToEcr uploadImages) {
            this.invokeCdk = invokeCdk;
            this.syncJars = syncJars;
            this.uploadImages = uploadImages;
        }

        @Override
        public void deployArtefactRepositories(String deploymentId) throws IOException, InterruptedException {
            invokeCdk.invoke(SleeperInternalCdkApp.ARTEFACTS, CdkCommand.deployArtefacts(deploymentId));
        }

        @Override
        public void uploadJars(SyncJarsRequest request) throws IOException {
            syncJars.sync(request);
        }

        @Override
        public void uploadImages(UploadDockerImagesToEcrRequest request) throws IOException, InterruptedException {
            uploadImages.upload(request);
        }

    }

}
