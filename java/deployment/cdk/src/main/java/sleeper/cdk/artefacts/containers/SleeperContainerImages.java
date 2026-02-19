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
package sleeper.cdk.artefacts.containers;

import software.constructs.Construct;

/**
 * Code to refer to container images during deployment.
 */
public interface SleeperContainerImages {

    /**
     * Creates a helper to deploy containers in ECS.
     *
     * @param  scope the scope you want to deploy in
     * @return       the helper
     */
    SleeperEcsImages ecsImagesAtScope(Construct scope);

    /**
     * Creates a helper to refer to container images during deployment to AWS Lambda.
     *
     * @param  scope the scope you want to deploy in
     * @return       the helper
     */
    SleeperLambdaImages lambdaImagesAtScope(Construct scope);

}
