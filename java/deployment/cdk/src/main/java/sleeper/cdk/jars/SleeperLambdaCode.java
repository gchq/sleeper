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
package sleeper.cdk.jars;

import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.DockerImageCode;
import software.amazon.awscdk.services.lambda.DockerImageFunction;
import software.amazon.awscdk.services.lambda.EcrImageCodeProps;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IVersion;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;

import java.util.List;
import java.util.function.Consumer;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;

public class SleeperLambdaCode {

    private final SleeperJarsInBucket jars;
    private final InstanceProperties instanceProperties;
    private final IBucket bucket;

    SleeperLambdaCode(SleeperJarsInBucket jars, InstanceProperties instanceProperties, IBucket bucket) {
        this.jars = jars;
        this.instanceProperties = instanceProperties;
        this.bucket = bucket;
    }

    public IVersion buildFunction(Construct scope, LambdaHandler handler, String id, Consumer<LambdaBuilder> config) {

        LambdaDeployType deployType = instanceProperties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        LambdaBuilder builder;
        if (deployType == LambdaDeployType.CONTAINER || handler.isAlwaysDockerDeploy()) {
            builder = new DockerFunctionBuilder(DockerImageFunction.Builder.create(scope, id)
                    .code(containerCode(scope, handler, id)));
        } else if (deployType == LambdaDeployType.JAR) {
            builder = new FunctionBuilder(Function.Builder.create(scope, id)
                    .code(jarCode(handler.getJar()))
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

    private Code jarCode(LambdaJar jar) {
        return Code.fromBucket(bucket, jar.getFilename(instanceProperties.get(VERSION)), jars.getLatestVersionId(jar));
    }

    private DockerImageCode containerCode(Construct scope, LambdaHandler handler, String id) {
        return DockerImageCode.fromEcr(
                Repository.fromRepositoryName(scope, id + "Repository", jars.getRepositoryName(handler.getJar())),
                EcrImageCodeProps.builder()
                        .cmd(List.of(handler.getHandler()))
                        .tagOrDigest(instanceProperties.get(VERSION))
                        .build());
    }
}
