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

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.constructs.Construct;

import sleeper.cdk.artefacts.UploadArtefactsAssets;
import sleeper.cdk.stack.core.LoggingStack;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;

public class CopyContainerImageStack extends NestedStack {
    private Provider provider;

    public CopyContainerImageStack(Construct scope, String id, InstanceProperties instanceProperties, UploadArtefactsAssets assets, LoggingStack loggingStack) {
        super(scope, id);
        create(Utils.cleanInstanceId(instanceProperties), assets,
                loggingStack.getLogGroup(LogGroupRef.COPY_CONTAINER),
                loggingStack.getLogGroup(LogGroupRef.COPY_CONTAINER_PROVIDER));
        Utils.addTags(this, instanceProperties);
    }

    private void create(String deploymentId, UploadArtefactsAssets assets, ILogGroup lambdaLogGroup, ILogGroup providerLogGroup) {
        IFunction lambda = assets.buildFunction(this, "Function", LambdaHandler.COPY_CONTAINER, builder -> builder
                .functionName("sleeper-" + deploymentId + "-copy-container")
                .memorySize(2048)
                .description("Lambda for copying container images from an external repository to ECR")
                .logGroup(lambdaLogGroup)
                .timeout(Duration.minutes(15)));

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
                .logGroup(providerLogGroup)
                .build();
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
