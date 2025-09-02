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
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
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
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AutoStopEcsClusterTasksStack extends NestedStack {

    public AutoStopEcsClusterTasksStack(Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars,
            NestedStack clusterStack, LoggingStack logging) {
        super(scope, id);

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "auto-stop-ecs-cluster-tasks");

        IFunction lambda = lambdaCode.buildFunction(this, LambdaHandler.AUTO_STOP_ECS_CLUSTER_TASKS, "AutoStopEcsClusterTasksLambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-stopping ECS tasks")
                .logGroup(logging.getLogGroup(LogGroupRef.ECS_CLUSTER_TASKS_AUTOSTOP))
                .timeout(Duration.minutes(10)));

        // Grant this function permission to list tasks and stop tasks
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("ecs:ListTasks", "ecs:StopTask", "iam:PassRole"))
                .build();
        IRole role = Objects.requireNonNull(lambda.getRole());
        role.addToPrincipalPolicy(policyStatement);
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        Provider propertiesWriterProvider = Provider.Builder.create(scope, id + "Provider")
                .onEventHandler(lambda)
                .logGroup(logging.getLogGroup(LogGroupRef.ECS_CLUSTER_TASKS_AUTOSTOP_PROVIDER))
                .build();

        CustomResource customResource = CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoStopEcsClusterTasks")
                .properties(Map.of("cluster", clusterStack.getNode().getId()))
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();

        customResource.getNode().addDependency(clusterStack);

        Utils.addStackTagIfSet(this, instanceProperties);

    }

}
