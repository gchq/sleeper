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
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.LambdaCode;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;

public class AutoDeleteS3Objects {

    private AutoDeleteS3Objects() {
    }

    public static void autoDeleteForBucket(
            Construct scope, InstanceProperties instanceProperties, LambdaCode lambdaCode,
            IBucket bucket, String bucketName,
            ILogGroup logGroup,
            ILogGroup providerLogGroup) {

        String functionName = bucketName + "-autodelete";
        String id = bucket.getNode().getId() + "-AutoDelete";

        IFunction lambda = lambdaCode.buildFunction(scope, LambdaHandler.AUTO_DELETE_S3_OBJECTS, id + "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(Utils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-deleting S3 objects")
                .logGroup(logGroup)
                .timeout(Duration.minutes(10)));

        bucket.grantRead(lambda);
        bucket.grantDelete(lambda);

        Provider propertiesWriterProvider = Provider.Builder.create(scope, id + "Provider")
                .onEventHandler(lambda)
                .logGroup(providerLogGroup)
                .build();

        CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoDeleteS3Objects")
                .properties(Map.of("bucket", bucketName))
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();
    }

}
