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
package sleeper.cdk.artefacts;

import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.DockerImageCode;
import software.amazon.awscdk.services.lambda.EcrImageCodeProps;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awssdk.services.s3.S3Client;
import software.constructs.Construct;

import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * Creates references to artefacts based on the instance properties. This must use the same InstanceProperties object
 * that is passed to SleeperInstance.
 */
public class SleeperArtefactsFromProperties implements SleeperArtefacts {

    private final InstanceProperties instanceProperties;
    private final SleeperJarVersionIdProvider jars;

    public SleeperArtefactsFromProperties(InstanceProperties instanceProperties, SleeperJarVersionIdProvider jars) {
        this.instanceProperties = instanceProperties;
        this.jars = jars;
    }

    public static SleeperArtefactsFromProperties from(S3Client s3Client, InstanceProperties instanceProperties) {
        return new SleeperArtefactsFromProperties(instanceProperties,
                SleeperJarVersionIdProvider.from(s3Client, instanceProperties));
    }

    @Override
    public SleeperLambdaCode lambdaCodeAtScope(Construct scope) {
        return new SleeperLambdaCode(scope, instanceProperties, lambdaJars(scope), lambdaImages(scope));
    }

    @Override
    public SleeperEcsImages ecsImagesAtScope(Construct scope) {
        Map<String, IRepository> deploymentNameToRepository = new HashMap<>();
        return deployment -> ContainerImage.fromEcrRepository(
                deploymentNameToRepository.computeIfAbsent(deployment.getDeploymentName(),
                        imageName -> createRepositoryReference(scope, deployment)),
                instanceProperties.get(VERSION));
    }

    private SleeperLambdaJars lambdaJars(Construct scope) {
        IBucket bucket = Bucket.fromBucketName(scope, "LambdaJarsBucket", instanceProperties.get(JARS_BUCKET));
        return jar -> Code.fromBucket(bucket, jar.getFilename(instanceProperties.get(VERSION)), jars.getLatestVersionId(jar));
    }

    private SleeperLambdaImages lambdaImages(Construct scope) {
        Map<String, IRepository> imageNameToRepository = new HashMap<>();
        return handler -> DockerImageCode.fromEcr(
                imageNameToRepository.computeIfAbsent(handler.getJar().getImageName(),
                        imageName -> createRepositoryReference(scope, handler.getJar())),
                EcrImageCodeProps.builder()
                        .cmd(List.of(handler.getHandler()))
                        .tagOrDigest(instanceProperties.get(VERSION))
                        .build());
    }

    private IRepository createRepositoryReference(Construct scope, LambdaJar jar) {
        String id = jar.getImageName() + "-repository";
        return Repository.fromRepositoryName(scope, id, jar.getEcrRepositoryName(instanceProperties));
    }

    private IRepository createRepositoryReference(Construct scope, DockerDeployment deployment) {
        String id = deployment.getDeploymentName() + "-repository";
        return Repository.fromRepositoryName(scope, id, deployment.getEcrRepositoryName(instanceProperties));
    }

}
