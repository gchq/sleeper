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
package sleeper.cdk.artefacts.jars;

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.constructs.Construct;

import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaHandler;

import java.util.List;
import java.util.Map;

public class CopyJarStack extends NestedStack {
    private final Provider provider;

    public CopyJarStack(Construct scope, String id, String deploymentId, SleeperJars jars) {
        super(scope, id);
        SleeperLambdaCode lambdaCode = SleeperLambdaCode.jarsOnly(this, jars);
        IFunction lambda = lambdaCode.buildFunction(LambdaHandler.COPY_CONTAINER, "Function", builder -> builder
                .functionName("sleeper-" + deploymentId + "-copy-jar")
                .memorySize(2048)
                .description("Lambda for copying jar files from an external repository to a jars bucket")
                .logGroup(LogGroup.Builder.create(this, "LambdaLogGroup")
                        .logGroupName("sleeper-" + deploymentId + "-copy-jar")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build())
                .timeout(Duration.minutes(15)));

        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .resources(List.of("arn:aws:s3:::sleeper-*-jars", "arn:aws:s3:::sleeper-*-jars/*"))
                .actions(List.of(
                        "s3:PutObject",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(LogGroup.Builder.create(this, "ProviderLogGroup")
                        .logGroupName("sleeper-" + deploymentId + "-copy-jar-provider")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build())
                .build();
    }

    /**
     * Creates a custom resource to copy a jar from a Maven repository.
     *
     * @param  scope  the stack to add the custom resource to
     * @param  id     the CDK construct ID for the custom resource
     * @param  source the source URL to retrieve the jar from
     * @param  bucket the S3 bucket name to copy to
     * @param  key    the key in the S3 bucket to copy to
     * @return        the custom resource
     */
    public CustomResource createCopyJar(Construct scope, String id, String source, String bucket, String key) {
        return CustomResource.Builder.create(scope, id)
                .resourceType("Custom::CopyJar")
                .properties(Map.of("source", source, "bucket", bucket, "key", key))
                .serviceToken(provider.getServiceToken())
                .build();
    }

}
