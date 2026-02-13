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
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.artefacts.SleeperLambdaImages;
import sleeper.cdk.artefacts.SleeperLambdaJars;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;

import java.util.function.Consumer;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;

public class SleeperLambdaCode {

    private final InstanceProperties instanceProperties;
    private final SleeperLambdaJars jars;
    private final SleeperLambdaImages images;
    private final IBucket bucket;

    private SleeperLambdaCode(InstanceProperties instanceProperties, SleeperLambdaJars jars, SleeperLambdaImages images, IBucket bucket) {
        this.instanceProperties = instanceProperties;
        this.jars = jars;
        this.images = images;
        this.bucket = bucket;
    }

    public static SleeperLambdaCode from(InstanceProperties instanceProperties, SleeperArtefacts artefacts, IBucket jarsBucket) {
        return new SleeperLambdaCode(instanceProperties, artefacts, artefacts, jarsBucket);
    }

    public static SleeperLambdaCode atScope(Construct scope, InstanceProperties instanceProperties, SleeperArtefacts artefacts) {
        return from(instanceProperties, artefacts, Bucket.fromBucketName(scope, "LambdaCodeBucket", instanceProperties.get(JARS_BUCKET)));
    }

    public IVersion buildFunction(Construct scope, LambdaHandler handler, String id, Consumer<LambdaBuilder> config) {

        LambdaDeployType deployType = instanceProperties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        LambdaBuilder builder;
        if (deployType == LambdaDeployType.CONTAINER || handler.isAlwaysDockerDeploy()) {
            builder = new DockerFunctionBuilder(DockerImageFunction.Builder.create(scope, id)
                    .code(images.containerCode(scope, handler, id)));
        } else if (deployType == LambdaDeployType.JAR) {
            builder = new FunctionBuilder(Function.Builder.create(scope, id)
                    .code(jars.jarCode(bucket, handler.getJar()))
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

    public IBucket getJarsBucket() {
        return bucket;
    }
}
