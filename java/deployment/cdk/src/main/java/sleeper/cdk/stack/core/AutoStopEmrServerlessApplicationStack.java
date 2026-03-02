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
import software.amazon.awscdk.services.emrserverless.CfnApplication;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

/**
 * Stops EMR Serverless application for the CloudFormation stack.
 */
public class AutoStopEmrServerlessApplicationStack extends NestedStack {

    private IFunction lambda;
    private Provider provider;

    public AutoStopEmrServerlessApplicationStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperInstanceArtefacts artefacts,
            LoggingStack loggingStack) {
        super(scope, id);
        createLambda(instanceProperties, artefacts, loggingStack);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void createLambda(InstanceProperties instanceProperties, SleeperInstanceArtefacts artefacts, LoggingStack loggingStack) {

        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "auto-stop-emr-serverless");

        lambda = lambdaCode.buildFunction(LambdaHandler.AUTO_STOP_EMR_SERVERLESS_APPLICATION, "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-stopping EMR Serverless application")
                .logGroup(loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_EMR_SERVERLESS_APPLICATION))
                .timeout(Duration.minutes(15)));

        // Grant this function permission to emrserverless actions
        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("emr-serverless:ListJobRuns", "emr-serverless:CancelJobRun", "emr-serverless:StopApplication",
                        "emr-serverless:GetApplication"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_EMR_SERVERLESS_APPLICATION_PROVIDER))
                .build();

        Utils.addTags(this, instanceProperties);

    }

    /**
     * Add a custom resource to stop EMR serverless applications.
     *
     * @param scope       the stack to add the custom resource to
     * @param application the EMR serverless application
     */
    public void addAutoStopEmrServerlessApplication(Construct scope, CfnApplication application) {

        String id = application.getNode().getId() + "-Autostop";

        CustomResource customResource = CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoStopEmrServerlessApplication")
                .properties(Map.of("applicationId", application.getAttrApplicationId()))
                .serviceToken(provider.getServiceToken())
                .build();

        customResource.getNode().addDependency(application);

    }

}
