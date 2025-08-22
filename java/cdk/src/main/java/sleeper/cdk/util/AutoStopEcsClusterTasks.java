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
package sleeper.cdk.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.ecs.ICluster;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.constructs.Construct;

import sleeper.cdk.jars.LambdaCode;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class AutoStopEcsClusterTasks {

    private AutoStopEcsClusterTasks() {
    }

    public static void autoStopTasksOnEcsCluster(
            Construct scope, InstanceProperties instanceProperties, LambdaCode lambdaCode,
            ICluster cluster, String clusterName,
            ILogGroup logGroup,
            ILogGroup providerLogGroup) {

        String id = cluster.getNode().getId() + "-AutoStop";
        String functionName = clusterName + "-autostop";

        IFunction lambda = lambdaCode.buildFunction(scope, LambdaHandler.AUTO_STOP_ECS_CLUSTER_TASKS, id + "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-stopping ECS tasks")
                .logGroup(logGroup)
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
                .logGroup(providerLogGroup)
                .build();

        CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoStopEcsClusterTasks")
                .properties(Map.of("cluster", clusterName))
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();
    }

}
