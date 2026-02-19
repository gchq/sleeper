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
package sleeper.cdk.lambda;

import software.amazon.awscdk.services.lambda.DockerImageFunction;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IVersion;
import software.amazon.awscdk.services.lambda.Runtime;
import software.constructs.Construct;

import sleeper.cdk.artefacts.containers.SleeperLambdaImages;
import sleeper.cdk.artefacts.jars.SleeperLambdaJars;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.model.LambdaDeployType;

import java.util.function.Consumer;

public class SleeperLambdaCode {

    private final Construct scope;
    private final LambdaDeployType deployType;
    private final SleeperLambdaJars jars;
    private final SleeperLambdaImages images;

    public SleeperLambdaCode(Construct scope, LambdaDeployType deployType, SleeperLambdaJars jars, SleeperLambdaImages images) {
        this.scope = scope;
        this.deployType = deployType;
        this.jars = jars;
        this.images = images;
    }

    public IVersion buildFunction(LambdaHandler handler, String id, Consumer<LambdaBuilder> config) {

        LambdaBuilder builder;
        if (deployType == LambdaDeployType.CONTAINER || handler.isAlwaysDockerDeploy()) {
            builder = new DockerFunctionBuilder(DockerImageFunction.Builder.create(scope, id)
                    .code(images.containerCode(handler)));
        } else if (deployType == LambdaDeployType.JAR) {
            builder = new FunctionBuilder(Function.Builder.create(scope, id)
                    .code(jars.jarCode(handler.getJar()))
                    .handler(handler.getHandler())
                    .runtime(Runtime.JAVA_17));
        } else {
            throw new IllegalArgumentException("Unrecognised lambda deploy type: " + deployType);
        }

        config.accept(builder);
        Function function = builder.build();

        // This is needed to tell the CDK to update the functions with new code when it changes in the jars bucket.
        // See the following:
        // https://www.define.run/posts/cdk-not-updating-lambda/
        // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
        // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
        return function.getCurrentVersion();
    }
}
