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
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.constructs.Construct;

import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaHandler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
public class CopyContainerImageProvider {
    private final Provider provider;

    private CopyContainerImageProvider(Builder builder) {
        Construct scope = Objects.requireNonNull(builder.scope, "scope must not be null");
        String id = Objects.requireNonNull(builder.id, "id must not be null");
        String functionName = Objects.requireNonNull(builder.functionName, "functionName must not be null");
        SleeperLambdaCode lambdaCode = Optional.ofNullable(builder.lambdaCode)
                .orElseGet(() -> SleeperLambdaCode.jarsOnly(scope, Objects.requireNonNull(builder.jars, "lambdaCode or jars must not be null")));
        ILogGroup functionLogGroup = Optional.ofNullable(builder.functionLogGroup)
                .orElseGet(() -> LogGroup.Builder.create(scope, id + "LambdaLogGroup")
                        .logGroupName(functionName)
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build());
        ILogGroup providerLogGroup = Optional.ofNullable(builder.providerLogGroup)
                .orElseGet(() -> LogGroup.Builder.create(scope, id + "ProviderLogGroup")
                        .logGroupName(functionName + "-provider")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build());

        IFunction lambda = lambdaCode.buildFunction(LambdaHandler.COPY_CONTAINER, id + "Function", lambdaBuilder -> lambdaBuilder
                .functionName(functionName)
                .memorySize(2048)
                .description("Lambda for copying container images from an external repository to ECR")
                .logGroup(functionLogGroup)
                .timeout(Duration.minutes(15)));

        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .resources(List.of("*"))
                .actions(List.of(
                        "ecr:GetAuthorizationToken",
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:CompleteLayerUpload",
                        "ecr:InitiateLayerUpload",
                        "ecr:PutImage",
                        "ecr:UploadLayerPart"))
                .build());

        provider = Provider.Builder.create(scope, id + "Provider")
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

    public static class Builder {
        private final Construct scope;
        private final String id;
        private String functionName;
        private SleeperLambdaCode lambdaCode;
        private SleeperJars jars;
        private ILogGroup functionLogGroup;
        private ILogGroup providerLogGroup;

        private Builder(Construct scope, String id) {
            this.scope = scope;
            this.id = id;
        }

        public static Builder create(Construct scope, String id) {
            return new Builder(scope, id);
        }

        public Builder functionName(String functionName) {
            this.functionName = functionName;
            return this;
        }

        public Builder lambdaCode(SleeperLambdaCode lambdaCode) {
            this.lambdaCode = lambdaCode;
            return this;
        }

        public Builder jars(SleeperJars jars) {
            this.jars = jars;
            return this;
        }

        public Builder functionLogGroup(ILogGroup functionLogGroup) {
            this.functionLogGroup = functionLogGroup;
            return this;
        }

        public Builder providerLogGroup(ILogGroup providerLogGroup) {
            this.providerLogGroup = providerLogGroup;
            return this;
        }

        public CopyContainerImageProvider build() {
            return new CopyContainerImageProvider(this);
        }
    }
}
