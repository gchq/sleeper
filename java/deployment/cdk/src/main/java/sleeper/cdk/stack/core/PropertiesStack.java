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
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperJarVersionIdsCache;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * The properties stack writes the Sleeper properties to S3 using a custom resource.
 */
public class PropertiesStack extends NestedStack {

    public PropertiesStack(
            Construct scope, String id, InstanceProperties instanceProperties, SleeperJarVersionIdsCache jars, SleeperCoreStacks coreStacks) {
        super(scope, id);

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        SleeperLambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "properties-writer");

        IFunction instancePropertiesWriterLambda = lambdaCode.buildFunction(this, LambdaHandler.INSTANCE_PROPERTIES_WRITER, "InstancePropertiesWriterLambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .description("Lambda for writing instance properties to S3 upon initialisation and teardown")
                .logGroup(coreStacks.getLogGroup(LogGroupRef.PROPERTIES_WRITER)));

        coreStacks.grantWriteInstanceConfig(instancePropertiesWriterLambda);

        Provider propertiesWriterProvider = Provider.Builder.create(this, "PropertiesWriterProvider")
                .onEventHandler(instancePropertiesWriterLambda)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.PROPERTIES_WRITER_PROVIDER))
                .build();

        CustomResource.Builder.create(this, "InstanceProperties")
                .resourceType("Custom::InstanceProperties")
                .properties(Map.of("properties", instanceProperties.saveAsString()))
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();

        Utils.addTags(this, instanceProperties);
    }
}
