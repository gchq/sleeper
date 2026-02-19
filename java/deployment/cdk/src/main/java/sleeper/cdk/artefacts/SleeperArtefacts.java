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

import software.amazon.awssdk.services.s3.S3Client;
import software.constructs.Construct;

import sleeper.cdk.artefacts.containers.SleeperContainerImages;
import sleeper.cdk.artefacts.containers.SleeperContainerImagesFromProperties;
import sleeper.cdk.artefacts.containers.SleeperEcsImages;
import sleeper.cdk.artefacts.jars.SleeperJarVersionIdProvider;
import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.artefacts.jars.SleeperJarsFromProperties;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;

import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;

/**
 * Points the CDK to deployment artefacts in AWS. This will include jars in the jars bucket, and Docker images in AWS
 * ECR.
 */
public class SleeperArtefacts {

    private final LambdaDeployType lambdaDeployType;
    private final SleeperJars jars;
    private final SleeperContainerImages containerImages;

    public SleeperArtefacts(LambdaDeployType lambdaDeployType, SleeperJars jars, SleeperContainerImages containerImages) {
        this.lambdaDeployType = lambdaDeployType;
        this.jars = jars;
        this.containerImages = containerImages;
    }

    /**
     * Creates references to artefacts based on the instance properties. This must use the same InstanceProperties
     * object that is passed to SleeperInstance.
     *
     * @param  s3Client           an S3 client to look up version IDs of jars in a versioned S3 jars bucket
     * @param  instanceProperties the instance properties
     * @return                    the artefacts
     */
    public static SleeperArtefacts from(S3Client s3Client, InstanceProperties instanceProperties) {
        return from(instanceProperties, SleeperJarVersionIdProvider.from(s3Client, instanceProperties));
    }

    /**
     * Creates references to artefacts based on the instance properties. This must use the same InstanceProperties
     * object that is passed to SleeperInstance.
     *
     * @param  instanceProperties the instance properties
     * @param  jars               a provider for the version IDs of the jars in a versioned S3 jars bucket
     * @return                    the artefacts
     */
    public static SleeperArtefacts from(InstanceProperties instanceProperties, SleeperJarVersionIdProvider jars) {
        return new SleeperArtefacts(
                instanceProperties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class),
                new SleeperJarsFromProperties(instanceProperties, jars),
                new SleeperContainerImagesFromProperties(instanceProperties));
    }

    /**
     * Creates a helper to deploy to AWS Lambda.
     *
     * @param  scope the scope you want to deploy in
     * @return       the helper
     */
    public SleeperLambdaCode lambdaCodeAtScope(Construct scope) {
        return new SleeperLambdaCode(scope, lambdaDeployType,
                jars.lambdaJarsAtScope(scope),
                containerImages.lambdaImagesAtScope(scope));
    }

    /**
     * Creates a helper to deploy containers in ECS.
     *
     * @param  scope the scope you want to deploy in
     * @return       the helper
     */
    public SleeperEcsImages ecsImagesAtScope(Construct scope) {
        return containerImages.ecsImagesAtScope(scope);
    }

}
