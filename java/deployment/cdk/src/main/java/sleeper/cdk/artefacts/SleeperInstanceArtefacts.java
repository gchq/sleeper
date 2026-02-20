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

import software.constructs.Construct;

import sleeper.cdk.artefacts.containers.SleeperContainerImages;
import sleeper.cdk.artefacts.containers.SleeperEcsImages;
import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;

import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;

/**
 * Points the CDK to deployment artefacts in AWS for a single Sleeper instance. This will include jars in the jars
 * bucket, and Docker images in AWS ECR.
 */
public class SleeperInstanceArtefacts {

    private final InstanceProperties instanceProperties;
    private final SleeperJars jars;
    private final SleeperContainerImages containerImages;

    public SleeperInstanceArtefacts(InstanceProperties instanceProperties, SleeperJars jars, SleeperContainerImages containerImages) {
        this.instanceProperties = instanceProperties;
        this.jars = jars;
        this.containerImages = containerImages;
    }

    /**
     * Creates a helper to deploy to AWS Lambda.
     *
     * @param  scope the scope you want to deploy in
     * @return       the helper
     */
    public SleeperLambdaCode lambdaCodeAtScope(Construct scope) {
        return new SleeperLambdaCode(scope,
                instanceProperties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class),
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
