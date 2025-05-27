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
import software.amazon.awscdk.CustomResourceProps;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.customresources.ProviderProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.PolicyStatementProps;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class VpcStack extends NestedStack {

    public VpcStack(Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars, LoggingStack logging) {
        super(scope, id);

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "vpc-check");

        IFunction vpcCheckLambda = lambdaCode.buildFunction(this, LambdaHandler.VPC_CHECK, "VpcCheckLambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .description("Lambda for checking the VPC has an associated S3 endpoint")
                .logGroup(logging.getLogGroup(LogGroupRef.VPC_CHECK)));

        vpcCheckLambda.addToRolePolicy(new PolicyStatement(new PolicyStatementProps.Builder()
                .actions(List.of("ec2:DescribeVpcEndpoints"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build()));

        //  Provider
        Provider provider = new Provider(this, "VpcCustomResourceProvider",
                ProviderProps.builder()
                        .onEventHandler(vpcCheckLambda)
                        .logGroup(logging.getLogGroup(LogGroupRef.VPC_CHECK_PROVIDER))
                        .build());

        // Custom resource to check whether VPC is valid
        new CustomResource(this, "VpcCheck", new CustomResourceProps.Builder()
                .resourceType("Custom::VpcCheck")
                .properties(Map.of(
                        "vpcId", instanceProperties.get(VPC_ID),
                        "region", instanceProperties.get(REGION)))
                .serviceToken(provider.getServiceToken())
                .build());

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
