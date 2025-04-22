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

import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;
import sleeper.core.properties.validation.OptionalStack;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildxImage;
import static sleeper.clients.deploy.StackDockerImage.emrServerlessImage;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

/**
 * This class is used to manage the Docker Images that need to be uploaded.
 */
public class DockerImageConfiguration {
    private static final Map<OptionalStack, StackDockerImage> DEFAULT_DOCKER_IMAGE_BY_STACK = Map.of(
            OptionalStack.IngestStack, dockerBuildImage(DockerDeployment.INGEST_NAME),
            OptionalStack.EksBulkImportStack, dockerBuildImage(DockerDeployment.EKS_BULK_IMPORT_NAME),
            OptionalStack.CompactionStack, dockerBuildxImage(DockerDeployment.COMPACTION_NAME),
            OptionalStack.EmrServerlessBulkImportStack, emrServerlessImage(DockerDeployment.EMR_SERVERLESS_BULK_IMPORT_NAME),
            OptionalStack.BulkExportStack, dockerBuildImage(DockerDeployment.BULK_EXPORT_NAME));

    private static final DockerImageConfiguration DEFAULT = new DockerImageConfiguration(DEFAULT_DOCKER_IMAGE_BY_STACK, LambdaHandler.all());

    private final Map<OptionalStack, StackDockerImage> imageByStack;
    private final List<LambdaHandler> lambdaHandlers;

    public static DockerImageConfiguration getDefault() {
        return DEFAULT;
    }

    public DockerImageConfiguration(Map<OptionalStack, StackDockerImage> imageByStack, List<LambdaHandler> lambdaHandlers) {
        this.imageByStack = imageByStack;
        this.lambdaHandlers = lambdaHandlers;
    }

    /**
     * This method returns a List of images to upload that haven't been uploaded before.
     * It does this by first getting a Set of Before values for Optional Stacks.
     * Then builds a list of OptionalStacks who don't match their before value.
     *
     * @param  properties The InstanceProperites to use to get the optional_stacks and the Lambda_deploy_type.
     * @param  diff       The PropertiesDiff used to get the properties before value.
     * @return            List of StackDockerImages that are new or have been updated to upload.
     */
    public List<StackDockerImage> getImagesToUploadOnUpdate(InstanceProperties properties, PropertiesDiff diff) {
        Set<OptionalStack> stacksBefore = diff.getValuesBefore(properties)
                .streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .collect(toUnmodifiableSet());
        List<OptionalStack> stacksAdded = properties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .filter(not(stacksBefore::contains))
                .collect(toUnmodifiableList());
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        return getImagesToUpload(stacksAdded, lambdaDeployType, lambda -> lambda.isDeployedOptional(stacksAdded));
    }

    /**
     * This method checks the optional stacks and lambda deploy types from the input properties.
     * It then concacts these lists together taking all of the Optional Stack and thos elambadas that are deployed on
     * one of the optional stacks.
     *
     * @param  properties The Instance Properties to check to get the Optional_Stacks and Lambda_deploy_Type.
     * @return            List of StackDockerImage concattenated from the optionals tacks and the lambdas that are
     *                    deployed on the optional stacks.
     */
    public List<StackDockerImage> getImagesToUpload(InstanceProperties properties) {
        List<OptionalStack> optionalStacks = properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class);
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        return getImagesToUpload(optionalStacks, lambdaDeployType, lambda -> lambda.isDeployed(optionalStacks));
    }

    private List<StackDockerImage> getImagesToUpload(
            Collection<OptionalStack> stacks, LambdaDeployType lambdaDeployType, Predicate<LambdaHandler> checkUploadLambda) {
        return Stream.concat(
                stacks.stream()
                        .map(this::getStackImage)
                        .flatMap(Optional::stream),
                lambdaImages(lambdaDeployType, checkUploadLambda))
                .collect(toUnmodifiableList());
    }

    private Stream<StackDockerImage> lambdaImages(LambdaDeployType lambdaDeployType, Predicate<LambdaHandler> checkUploadLambda) {
        if (lambdaDeployType != LambdaDeployType.CONTAINER) {
            return Stream.empty();
        }
        return lambdaHandlers.stream().filter(checkUploadLambda)
                .map(LambdaHandler::getJar).distinct()
                .map(StackDockerImage::lambdaImage);
    }

    private Optional<StackDockerImage> getStackImage(OptionalStack stack) {
        return Optional.ofNullable(imageByStack.get(stack));
    }

    /**
     * This method uses the stored map and inputted repo name to find the instanceId of the image as it will be part of
     * the image name.
     *
     * @param  repositoryName The repositoryName to search the map for the first occurance of.
     * @return                Optional String of the InstanceId provided the repositoryName was found in the map.
     */
    public Optional<String> getInstanceIdFromRepoName(String repositoryName) {
        return imageByStack.values().stream()
                .filter(image -> repositoryName.endsWith("/" + image.getImageName()))
                .map(image -> repositoryName.substring(0, repositoryName.indexOf("/")))
                .findFirst();
    }
}
