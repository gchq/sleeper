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

package sleeper.systemtest.cdk;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.ContainerInsights;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.s3.Bucket;
import software.constructs.Construct;

import sleeper.cdk.networking.SleeperNetworking;
import sleeper.cdk.stack.core.EcsClusterTasksStack;
import sleeper.cdk.util.Utils;
import sleeper.core.SleeperVersion;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;
import sleeper.systemtest.configuration.SystemTestConstants;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertySetter;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_LOG_RETENTION_DAYS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_TASK_CPU;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_TASK_MEMORY;
import static sleeper.systemtest.configuration.SystemTestProperty.WRITE_DATA_TASK_DEFINITION_FAMILY;

public class SystemTestClusterStack extends NestedStack {

    public SystemTestClusterStack(
            Construct scope, String id, SystemTestStandaloneProperties properties, SleeperNetworking networking,
            SystemTestBucketStack bucketStack, EcsClusterTasksStack autoStopEcsClusterTasksStack) {
        super(scope, id);
        create(properties, properties, properties.toInstancePropertiesForCdkUtils(), networking, bucketStack, autoStopEcsClusterTasksStack);
        Tags.of(this).add("DeploymentStack", id);
    }

    public SystemTestClusterStack(
            Construct scope, String id, SystemTestProperties properties, SleeperNetworking networking,
            SystemTestBucketStack bucketStack, EcsClusterTasksStack autoStopEcsClusterTasksStack) {
        super(scope, id);
        create(properties.testPropertiesOnly(), properties::set, properties, networking, bucketStack, autoStopEcsClusterTasksStack);
        Utils.addTags(this, properties);
    }

    private void create(
            SystemTestPropertyValues properties, SystemTestPropertySetter propertySetter,
            InstanceProperties instanceProperties, SleeperNetworking networking,
            SystemTestBucketStack bucketStack, EcsClusterTasksStack autoStopEcsClusterTasksStack) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);

        // ECS cluster for tasks to write data
        String clusterName = String.join("-", "sleeper", instanceId, "system-test-cluster");
        Cluster cluster = Cluster.Builder
                .create(this, "SystemTestCluster")
                .clusterName(clusterName)
                .containerInsightsV2(ContainerInsights.ENHANCED)
                .vpc(networking.vpc())
                .build();
        propertySetter.set(SYSTEM_TEST_CLUSTER_NAME, cluster.getClusterName());
        CfnOutputProps writeClusterOutputProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, "systemTestClusterName", writeClusterOutputProps);

        autoStopEcsClusterTasksStack.addAutoStopEcsClusterTasks(this, cluster);

        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder
                .create(this, "TaskDefinition")
                .family(instanceProperties.get(ID) + "SystemTestTaskFamily")
                .cpu(properties.getInt(SYSTEM_TEST_TASK_CPU))
                .memoryLimitMiB(properties.getInt(SYSTEM_TEST_TASK_MEMORY))
                .build();
        IRole taskRole = taskDefinition.getTaskRole();
        propertySetter.set(WRITE_DATA_TASK_DEFINITION_FAMILY, taskDefinition.getFamily());
        CfnOutputProps taskDefinitionFamilyOutputProps = new CfnOutputProps.Builder()
                .value(taskDefinition.getFamily())
                .build();
        new CfnOutput(this, "systemTestTaskDefinitionFamily", taskDefinitionFamilyOutputProps);

        IRepository repository = Repository.fromRepositoryName(this, "SystemTestECR", properties.get(SYSTEM_TEST_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, SleeperVersion.getVersion());

        String logGroupName = String.join("-", "sleeper", instanceId, "SystemTestTasks");
        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(LogDriver.awsLogs(AwsLogDriverProps.builder()
                        .streamPrefix(logGroupName)
                        .logGroup(LogGroup.Builder.create(this, "SystemTestTasks")
                                .logGroupName(logGroupName)
                                .retention(Utils.getRetentionDays(properties.getInt(SYSTEM_TEST_LOG_RETENTION_DAYS)))
                                .removalPolicy(Utils.logsRemovalPolicy(instanceProperties))
                                .build())
                        .build()))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .build();
        taskDefinition.addContainer(SystemTestConstants.SYSTEM_TEST_CONTAINER, containerDefinitionOptions);

        Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET)).grantRead(taskRole);
        bucketStack.getBucket().grantReadWrite(taskRole);
        taskRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("sts:AssumeRole"))
                .resources(List.of("arn:aws:iam::*:role/sleeper-ingest-*"))
                .build());
    }
}
