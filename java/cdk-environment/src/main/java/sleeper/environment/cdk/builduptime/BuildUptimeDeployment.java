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
package sleeper.environment.cdk.builduptime;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IFunction;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.OptionalStringParameter;

import java.util.List;
import java.util.Map;

import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;
import static software.amazon.awscdk.services.lambda.Runtime.JAVA_11;

public class BuildUptimeDeployment {
    public static final OptionalStringParameter LAMBDA_JAR = AppParameters.BUILD_UPTIME_LAMBDA_JAR;

    private final IFunction function;

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public BuildUptimeDeployment(Construct scope) {
        AppContext context = AppContext.of(scope);
        String lambdaJarPath = context.get(LAMBDA_JAR)
                .orElseThrow(() -> new IllegalArgumentException("buildUptimeLambdaJar is required for BuildUptimeStack"));

        function = Function.Builder.create(scope, "BuildUptimeFunction")
                .code(Code.fromAsset(lambdaJarPath))
                .functionName("sleeper-" + context.get(INSTANCE_ID) + "-build-uptime")
                .description("Start and stop EC2 instances and schedule rules")
                .runtime(JAVA_11)
                .memorySize(1024)
                .timeout(Duration.minutes(10))
                .handler("sleeper.build.uptime.lambda.BuildUptimeLambda::handleRequest")
                .environment(Map.of())
                .reservedConcurrentExecutions(1)
                .build().getCurrentVersion();

        function.getRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .resources(List.of("*"))
                .actions(List.of(
                        "ec2:StartInstances", "ec2:StopInstances",
                        "events:EnableRule", "events:DisableRule"))
                .build());
    }

    public IFunction getFunction() {
        return function;
    }
}
