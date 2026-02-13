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

import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.s3.IBucket;

import sleeper.core.deploy.LambdaJar;

/**
 * Code to refer to a fat jar for use when deploying a lambda. Sleeper builds fat jars that include all the dependencies
 * for a lambda.
 */
@FunctionalInterface
public interface SleeperLambdaJars {

    /**
     * Retrieves a reference to a fat jar in the jars bucket.
     *
     * @param  jarsBucket a reference to the jars bucket in the scope of the current stack
     * @param  jar        which jar we want to reference
     * @return            the reference to the jar
     */
    Code jarCode(IBucket jarsBucket, LambdaJar jar);
}
