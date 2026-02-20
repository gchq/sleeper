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
package sleeper.cdk.artefacts.containers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.ec2.Subnet;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.constructs.Construct;

import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.lambda.LambdaBuilder;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", "MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR"})
public class CopyContainerImageStack extends NestedStack {
    private Provider provider;

    public CopyContainerImageStack(Construct scope, String id, String deploymentId, SleeperJars jars, String vpcId, String subnetIds) {
        super(scope, id);
        SleeperLambdaCode lambdaCode = SleeperLambdaCode.jarsOnly(this, jars);
        IFunction lambda = lambdaCode.buildFunction(LambdaHandler.COPY_CONTAINER, "Function", withVpc(vpcId, subnetIds).andThen(builder -> builder
                .functionName("sleeper-" + deploymentId + "-copy-container")
                .memorySize(2048)
                .description("Lambda for copying container images from an external repository to ECR")
                .logGroup(LogGroup.Builder.create(this, "LambdaLogGroup")
                        .logGroupName("sleeper-" + deploymentId + "-copy-container")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build())
                .vpcSubnets(null)
                .timeout(Duration.minutes(15))));

        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .resources(List.of("*"))
                .actions(List.of(
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:CompleteLayerUpload",
                        "ecr:InitiateLayerUpload",
                        "ecr:PutImage",
                        "ecr:UploadLayerPart"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(LogGroup.Builder.create(this, "ProviderLogGroup")
                        .logGroupName("sleeper-" + deploymentId + "-copy-container-provider")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build())
                .build();
    }

    private Consumer<LambdaBuilder> withVpc(String vpcId, String subnetIds) {
        if (vpcId == null) {
            return builder -> {
            };
        }
        return builder -> builder
                .vpc(Vpc.fromLookup(this, "Vpc", VpcLookupOptions.builder().vpcId(vpcId).build()))
                .vpcSubnets(SubnetSelection.builder()
                        .subnets(SleeperPropertyValueUtils.readList(subnetIds).stream()
                                .map(id -> Subnet.fromSubnetId(this, "Subnet-" + id, id))
                                .toList())
                        .build());
    }

    /**
     * Creates a custom resource to copy an image.
     *
     * @param  scope  the stack to add the custom resource to
     * @param  id     the CDK construct ID for the custom resource
     * @param  source the source image name, including repository and path
     * @param  target the target image name, including repository and path
     * @return        the custom resource
     */
    public CustomResource createCopyContainerImage(Construct scope, String id, String source, String target) {
        return CustomResource.Builder.create(scope, id)
                .resourceType("Custom::CopyContainerImage")
                .properties(Map.of("source", source, "target", target))
                .serviceToken(provider.getServiceToken())
                .build();
    }

}
