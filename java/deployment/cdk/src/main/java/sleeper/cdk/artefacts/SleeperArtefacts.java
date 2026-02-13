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

import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.DockerDeployment;

/**
 * Points the CDK to deployment artefacts in AWS. This will include jars in the jars bucket, and Docker images in AWS
 * ECR.
 */
public interface SleeperArtefacts {

    /**
     * Creates a helper to deploy lambdas using these deployment artefacts.
     *
     * @param  scope the scope you want to deploy in
     * @return       the helper
     */
    SleeperLambdaCode lambdaCodeAtScope(Construct scope);

    /**
     * Creates a helper to deploy containers in ECS.
     *
     * @param  scope the scope you want to deploy in
     * @return       the helper
     */
    SleeperEcsImages ecsImagesAtScope(Construct scope);

    /**
     * Retrieves the Docker image name for a deployment. Includes the ECR repository URL and the tag.
     *
     * @param  deployment which deployment we want the Docker image for
     * @return            the Docker image name
     */
    String imageName(DockerDeployment deployment);
}
