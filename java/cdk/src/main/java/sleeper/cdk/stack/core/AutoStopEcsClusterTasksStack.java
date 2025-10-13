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
import software.amazon.awscdk.services.ecs.ICluster;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
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

/**
 * Stops ECS Cluster tasks for the CloudFormation stack.
 */
@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class AutoStopEcsClusterTasksStack extends NestedStack {

    private IFunction lambda;
    private Provider provider;

    public AutoStopEcsClusterTasksStack(Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars,
            LoggingStack loggingStack) {
        super(scope, id);
        createLambda(instanceProperties, jars, loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS),
                loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS_PROVIDER));
    }

    public AutoStopEcsClusterTasksStack(Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars) {
        super(scope, id);
        ILogGroup logGroup = LoggingStack.createLogGroup(this, LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS, instanceProperties);
        ILogGroup providerLogGroup = LoggingStack.createLogGroup(this, LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS_PROVIDER, instanceProperties);
        createLambda(instanceProperties, jars, logGroup, providerLogGroup);
    }

    private void createLambda(InstanceProperties instanceProperties, BuiltJars jars, ILogGroup logGroup, ILogGroup providerLogGroup) {

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "auto-stop-ecs-cluster-tasks");

        lambda = lambdaCode.buildFunction(this, LambdaHandler.AUTO_STOP_ECS_CLUSTER_TASKS, "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-stopping ECS tasks")
                .logGroup(logGroup)
                .timeout(Duration.minutes(15)));

        IRole role = Objects.requireNonNull(lambda.getRole());
        role.addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("ecs:ListTasks", "ecs:StopTask"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(providerLogGroup)
                .build();

        Utils.addStackTagIfSet(this, instanceProperties);

    }

    /**
     * Allow the stack to stop cluster tasks.
     *
     * @param scope   the stack to add the custom resource to
     * @param cluster the ECS cluster
     */
    public void addAutoStopEcsClusterTasks(Construct scope, ICluster cluster) {

        String id = cluster.getNode().getId() + "-AutoStop";

        CustomResource customResource = CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoStopEcsClusterTasks")
                .properties(Map.of("cluster", cluster.getClusterName()))
                .serviceToken(provider.getServiceToken())
                .build();

        customResource.getNode().addDependency(cluster);

    }

}
