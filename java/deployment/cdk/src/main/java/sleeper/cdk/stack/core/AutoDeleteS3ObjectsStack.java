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
package sleeper.cdk.stack.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperJarsInBucket;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

/**
 * Delete's S3 objects for a CloudFormation stack.
 */
public class AutoDeleteS3ObjectsStack extends NestedStack {

    private IFunction lambda;
    private Provider provider;

    public AutoDeleteS3ObjectsStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperJarsInBucket jars, LoggingStack loggingStack) {
        super(scope, id);
        createLambda(instanceProperties, jars, loggingStack.getLogGroup(LogGroupRef.AUTO_DELETE_S3_OBJECTS),
                loggingStack.getLogGroup(LogGroupRef.AUTO_DELETE_S3_OBJECTS_PROVIDER));
    }

    // This is for a standalone system test deployment, where there is no Sleeper instance and the LoggingStack is not used.
    public AutoDeleteS3ObjectsStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperJarsInBucket jars) {
        super(scope, id);
        ILogGroup logGroup = LoggingStack.createLogGroup(this, LogGroupRef.AUTO_DELETE_S3_OBJECTS, instanceProperties);
        ILogGroup providerLogGroup = LoggingStack.createLogGroup(this, LogGroupRef.AUTO_DELETE_S3_OBJECTS_PROVIDER, instanceProperties);
        createLambda(instanceProperties, jars, logGroup, providerLogGroup);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE") // getRole is incorrectly labelled as nullable
    private void createLambda(InstanceProperties instanceProperties, SleeperJarsInBucket jars, ILogGroup logGroup, ILogGroup providerLogGroup) {

        // Jars bucket
        IBucket jarsBucket = jars.createJarsBucketReference(this, "JarsBucket");
        SleeperLambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "auto-delete-s3-objects");

        lambda = lambdaCode.buildFunction(this, LambdaHandler.AUTO_DELETE_S3_OBJECTS, "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-deleting S3 objects")
                .logGroup(logGroup)
                .timeout(Duration.minutes(15)));

        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("s3:ListBucketVersions", "s3:DeleteObject", "s3:DeleteObjectVersion"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(providerLogGroup)
                .build();

        Utils.addTags(this, instanceProperties);

    }

    /**
     * Adds a custom resource to delete a bucket's contents.
     *
     * @param scope  the stack to add the custom resource to
     * @param bucket the bucket to delete from
     */
    public void addAutoDeleteS3Objects(Construct scope, IBucket bucket) {

        String id = bucket.getNode().getId() + "-AutoDelete";

        CustomResource customResource = CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoDeleteS3Objects")
                .properties(Map.of("bucket", bucket.getBucketName()))
                .serviceToken(provider.getServiceToken())
                .build();

        customResource.getNode().addDependency(bucket);
    }

}
