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
package sleeper.cdk.stack;

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * The properties stack writes the Sleeper properties to S3 using a custom resource.
 */
public class PropertiesStack extends NestedStack {

    public PropertiesStack(
            Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars, CoreStacks coreStacks) {
        super(scope, id);

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("properties", instanceProperties.saveAsString());

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "properties-writer");

        IFunction propertiesWriterLambda = lambdaCode.buildFunction(this, LambdaHandler.PROPERTIES_WRITER, "PropertiesWriterLambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .description("Lambda for writing instance properties to S3 upon initialisation and teardown")
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName)));

        coreStacks.grantWriteInstanceConfig(propertiesWriterLambda);

        Provider propertiesWriterProvider = Provider.Builder.create(this, "PropertiesWriterProvider")
                .onEventHandler(propertiesWriterLambda)
                .logGroup(coreStacks.getProviderLogGroupByFunctionName(functionName))
                .build();

        CustomResource.Builder.create(this, "InstanceProperties")
                .resourceType("Custom::InstanceProperties")
                .properties(properties)
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
