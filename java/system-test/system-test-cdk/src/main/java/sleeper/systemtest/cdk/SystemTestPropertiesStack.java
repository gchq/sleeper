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

package sleeper.systemtest.cdk;

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.cdk.Utils.createLogGroupWithRetentionDays;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JARS_BUCKET;

public class SystemTestPropertiesStack extends NestedStack {

    public SystemTestPropertiesStack(
            Construct scope, String id, SystemTestStandaloneProperties systemTestProperties,
            SystemTestBucketStack bucketStack, BuiltJars jars) {
        super(scope, id);

        String jarsBucketName = systemTestProperties.get(SYSTEM_TEST_JARS_BUCKET);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jarsBucketName);
        LambdaCode jar = jars.lambdaCode(BuiltJar.CUSTOM_RESOURCES, jarsBucket);

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("properties", systemTestProperties.saveAsString());

        String functionName = String.join("-", "sleeper",
                systemTestProperties.get(SYSTEM_TEST_ID).toLowerCase(Locale.ROOT), "properties-writer");

        IFunction propertiesWriterLambda = jar.buildFunction(this, "PropertiesWriterLambda", builder -> builder
                .functionName(functionName)
                .handler("sleeper.cdk.custom.PropertiesWriterLambda::handleEvent")
                .memorySize(2048)
                .environment(Map.of(CONFIG_BUCKET.toEnvironmentVariable(), bucketStack.getBucket().getBucketName()))
                .description("Lambda for writing system test properties to S3 upon initialisation and teardown")
                .logGroup(createLogGroupWithRetentionDays(this, "PropertiesWriterLambdaLogGroup", 30))
                .runtime(Runtime.JAVA_11));

        bucketStack.getBucket().grantWrite(propertiesWriterLambda);

        Provider propertiesWriterProvider = Provider.Builder.create(this, "PropertiesWriterProvider")
                .onEventHandler(propertiesWriterLambda)
                .logGroup(createLogGroupWithRetentionDays(this, "PropertiesWriterProviderLogGroup", 30))
                .build();

        CustomResource.Builder.create(this, "SystemTestProperties")
                .resourceType("Custom::SystemTestProperties")
                .properties(properties)
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();
        Tags.of(this).add("DeploymentStack", id);
    }
}
