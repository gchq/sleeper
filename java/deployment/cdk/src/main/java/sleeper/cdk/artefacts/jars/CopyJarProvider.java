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
package sleeper.cdk.artefacts.jars;

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

import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaHandler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
public class CopyJarProvider {
    private final Provider provider;

    private CopyJarProvider(Builder builder) {
        Construct scope = Objects.requireNonNull(builder.scope, "scope must not be null");
        String id = Objects.requireNonNull(builder.id, "id must not be null");
        SleeperJars jars = Objects.requireNonNull(builder.jars, "jars must not be null");
        String functionName = Objects.requireNonNull(builder.functionName, "functionName must not be null");
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

        SleeperLambdaCode lambdaCode = SleeperLambdaCode.jarsOnly(scope, jars);
        IFunction lambda = lambdaCode.buildFunction(LambdaHandler.COPY_JAR, id + "Function", lambdaBuilder -> lambdaBuilder
                .functionName(functionName)
                .memorySize(2048)
                .description("Lambda for copying jar files from an external repository to a jars bucket")
                .logGroup(functionLogGroup)
                .timeout(Duration.minutes(15)));

        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .resources(List.of("arn:aws:s3:::sleeper-*-jars", "arn:aws:s3:::sleeper-*-jars/*"))
                .actions(List.of(
                        "s3:PutObject",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload"))
                .build());

        provider = Provider.Builder.create(scope, id + "Provider")
                .onEventHandler(lambda)
                .logGroup(providerLogGroup)
                .build();
    }

    /**
     * Creates a custom resource to copy a jar from a Maven repository.
     *
     * @param  scope  the stack to add the custom resource to
     * @param  id     the CDK construct ID for the custom resource
     * @param  source the source URL to retrieve the jar from
     * @param  bucket the S3 bucket name to copy to
     * @param  key    the key in the S3 bucket to copy to
     * @return        the custom resource
     */
    public CustomResource createCopyJar(Construct scope, String id, String source, String bucket, String key) {
        return CustomResource.Builder.create(scope, id)
                .resourceType("Custom::CopyJar")
                .properties(Map.of("source", source, "bucket", bucket, "key", key))
                .serviceToken(provider.getServiceToken())
                .build();
    }

    public static class Builder {
        private final Construct scope;
        private final String id;
        private String functionName;
        private ILogGroup functionLogGroup;
        private ILogGroup providerLogGroup;
        private SleeperJars jars;

        private Builder(Construct scope, String id) {
            this.scope = scope;
            this.id = id;
        }

        public static Builder create(Construct scope, String id) {
            return new Builder(scope, id);
        }

        public Builder jars(SleeperJars jars) {
            this.jars = jars;
            return this;
        }

        public Builder functionName(String functionName) {
            this.functionName = functionName;
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

        public CopyJarProvider build() {
            return new CopyJarProvider(this);
        }
    }

}
