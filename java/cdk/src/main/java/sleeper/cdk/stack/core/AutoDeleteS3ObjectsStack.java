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

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.Map;

public class AutoDeleteS3ObjectsStack extends NestedStack {

    private IFunction lambda;
    private Provider provider;

    public AutoDeleteS3ObjectsStack(Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars, LoggingStack loggingStack) {
        super(scope, id);
        createLambda(id, instanceProperties, jars, loggingStack.getLogGroup(LogGroupRef.AUTO_DELETE_S3_OBJECTS),
                loggingStack.getLogGroup(LogGroupRef.AUTO_DELETE_S3_OBJECTS_PROVIDER));
    }

    public AutoDeleteS3ObjectsStack(Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars, Integer retentionDays) {
        super(scope, id);
        ILogGroup logGroup = LogGroup.Builder.create(this, id + "-AutoDeleteLambdaLogGroup")
                .logGroupName("s3-bucket-autodelete")
                .retention(Utils.getRetentionDays(retentionDays))
                .build();
        ILogGroup providerLogGroup = LogGroup.Builder.create(this, id + "-AutoDeleteProviderLogGroup")
                .logGroupName("s3-bucket-autodelete-provider")
                .retention(Utils.getRetentionDays(retentionDays))
                .build();
        createLambda(id, instanceProperties, jars, logGroup, providerLogGroup);
    }

    private void createLambda(String id, InstanceProperties instanceProperties, BuiltJars jars, ILogGroup logGroup, ILogGroup providerLogGroup) {

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "auto-delete-s3-objects");

        lambda = lambdaCode.buildFunction(this, LambdaHandler.AUTO_DELETE_S3_OBJECTS, id + "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-deleting S3 objects")
                .logGroup(logGroup)
                .timeout(Duration.minutes(10)));

        provider = Provider.Builder.create(this, id + "Provider")
                .onEventHandler(lambda)
                .logGroup(providerLogGroup)
                .build();
    }

    public void grantAccessToCustomResource(String id, InstanceProperties instanceProperties,
            IBucket bucket, String bucketName) {

        bucket.grantRead(lambda);
        bucket.grantDelete(lambda);

        CustomResource.Builder.create(this, id + "-AutoDelete")
                .resourceType("Custom::AutoDeleteS3Objects")
                .properties(Map.of("bucket", bucketName))
                .serviceToken(provider.getServiceToken())
                .build();
    }

}
