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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.jars.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

public class PauseScheduledRuleStack extends NestedStack {

    private IFunction lambda;
    private Provider provider;

    public PauseScheduledRuleStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperJarsInBucket jars,
            LoggingStack loggingStack) {
        super(scope, id);
        createLambda(instanceProperties, jars, loggingStack);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE") // getRole is incorrectly labelled as nullable
    private void createLambda(InstanceProperties instanceProperties, SleeperJarsInBucket jars, LoggingStack loggingStack) {

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        SleeperLambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "pause-scheduled-rules");

        lambda = lambdaCode.buildFunction(this, LambdaHandler.PAUSE_SCHEDULED_RULE, "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for pausing scheduled rules")
                .logGroup(loggingStack.getLogGroup(LogGroupRef.PAUSE_SCHEDULED_RULES))
                .timeout(Duration.minutes(15)));

        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("cwe:DisableRule"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(loggingStack.getLogGroup(LogGroupRef.PAUSE_SCHEDULED_RULES_PROVIDER))
                .build();

        Utils.addStackTagIfSet(this, instanceProperties);

    }

    /**
     * Adds a custom resource to pause scheduled rules.
     *
     * @param scope    the stack to add the custom resource to
     * @param autoStop the auto stop stack
     */
    public void addPauseScheduledRule(Construct scope, NestedStack autoStop) {

        String id = autoStop.getNode().getId() + "-PauseRule";

        CustomResource customResource = CustomResource.Builder.create(scope, id)
                .resourceType("Custom::PauseScheduledRule")
                .properties(Map.of("autoStopStack", autoStop.getStackName()))
                .serviceToken(provider.getServiceToken())
                .build();

        customResource.getNode().addDependency(autoStop);

    }
}
