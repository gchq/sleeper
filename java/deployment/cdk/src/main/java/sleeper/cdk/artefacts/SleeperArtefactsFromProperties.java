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
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * Creates references to artefacts based on the instance properties. This must use the same InstanceProperties object
 * that is passed to SleeperInstance.
 */
public class SleeperArtefactsFromProperties implements SleeperArtefacts, SleeperEcsImages, SleeperDockerImageNames {

    private final InstanceProperties instanceProperties;
    private final SleeperJarVersionIdsCache jars;

    public SleeperArtefactsFromProperties(InstanceProperties instanceProperties, SleeperJarVersionIdsCache jars) {
        this.instanceProperties = instanceProperties;
        this.jars = jars;
    }

    public static SleeperArtefactsFromProperties from(S3Client s3Client, InstanceProperties instanceProperties) {
        return new SleeperArtefactsFromProperties(instanceProperties,
                SleeperJarVersionIdsCache.from(s3Client, instanceProperties));
    }

    @Override
    public DockerImageCode containerCode(Construct scope, LambdaHandler handler, String id) {
        return DockerImageCode.fromEcr(
                Repository.fromRepositoryName(scope, id + "Repository", jars.getRepositoryName(handler.getJar())),
                EcrImageCodeProps.builder()
                        .cmd(List.of(handler.getHandler()))
                        .tagOrDigest(instanceProperties.get(VERSION))
                        .build());
    }

    @Override
    public Code jarCode(IBucket jarsBucket, LambdaJar jar) {
        return Code.fromBucket(jarsBucket, jar.getFilename(instanceProperties.get(VERSION)), jars.getLatestVersionId(jar));
    }

    @Override
    public SleeperLambdaCode lambdaCodeAtScope(Construct scope) {
        IBucket bucket = Bucket.fromBucketName(scope, "LambdaCodeBucket", instanceProperties.get(JARS_BUCKET));
        return new SleeperLambdaCode(instanceProperties, this, this, bucket);
    }

    @Override
    public ContainerImage containerImage(Construct scope, DockerDeployment deployment, String id) {
        IRepository repository = Repository.fromRepositoryName(scope, id, deployment.getEcrRepositoryName(instanceProperties));
        return ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));
    }

    @Override
    public String imageName(DockerDeployment deployment) {
        return deployment.getDockerImageName(instanceProperties);
    }

}
