/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.cdk.util;

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;

import static sleeper.cdk.util.Utils.createCustomResourceProviderLogGroup;
import static sleeper.cdk.util.Utils.createLambdaLogGroup;

public class AutoDeleteS3Objects {

    private AutoDeleteS3Objects() {
    }

    public static void autoDeleteForBucket(Construct scope, BuiltJars jars, InstanceProperties instanceProperties, IBucket bucket) {
        IBucket jarsBucket = Bucket.fromBucketName(scope, "JarsBucket", jars.bucketName());
        LambdaCode jar = jars.lambdaCode(BuiltJar.CUSTOM_RESOURCES, jarsBucket);
        autoDeleteForBucket(scope, jar, instanceProperties, bucket);
    }

    public static void autoDeleteForBucket(Construct scope, LambdaCode customResourcesJar, InstanceProperties instanceProperties, IBucket bucket) {

        String id = bucket.getNode().getId() + "-AutoDelete";
        String functionName = bucket.getBucketName() + "-autodelete";

        IFunction lambda = customResourcesJar.buildFunction(scope, id + "Lambda", builder -> builder
                .functionName(functionName)
                .handler("sleeper.cdk.custom.AutoDeleteS3ObjectsLambda::handleEvent")
                .memorySize(2048)
                .environment(Utils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-deleting S3 objects")
                .logGroup(createLambdaLogGroup(scope, id + "LambdaLogGroup", functionName, instanceProperties))
                .runtime(Runtime.JAVA_11));

        bucket.grantDelete(lambda);

        Provider propertiesWriterProvider = Provider.Builder.create(scope, id + "Provider")
                .onEventHandler(lambda)
                .logGroup(createCustomResourceProviderLogGroup(scope, id + "ProviderLogGroup", functionName, instanceProperties))
                .build();

        CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoDeleteS3Objects")
                .properties(Map.of("bucket", bucket.getBucketName()))
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();
    }

}
