/*
 * Copyright 2022-2023 Crown Copyright
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
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.core.SleeperVersion;
import sleeper.systemtest.configuration.SystemTestConstants;
import sleeper.systemtest.configuration.SystemTestProperty;
import sleeper.systemtest.configuration.SystemTestPropertySetter;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.Locale;

import static sleeper.cdk.Utils.getRetentionDays;
import static sleeper.systemtest.cdk.SystemTestStack.generateSystemTestClusterName;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_TASK_CPU;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_TASK_MEMORY;
import static sleeper.systemtest.configuration.SystemTestProperty.WRITE_DATA_ROLE_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.WRITE_DATA_TASK_DEFINITION_FAMILY;

public class SystemTestClusterStack extends NestedStack {

    public SystemTestClusterStack(Construct scope, String id,
                                  SystemTestStandaloneProperties properties,
                                  SystemTestBucketStack bucketStack) {
        super(scope, id);
        createSystemTestCluster(this, properties, bucketStack);
        Tags.of(this).add("DeploymentStack", getNode().getId());
    }

    public static void createSystemTestCluster(Construct scope,
                                               SystemTestStandaloneProperties properties,
                                               SystemTestBucketStack bucketStack) {
        createSystemTestCluster(scope, properties, properties,
                properties.get(SystemTestProperty.SYSTEM_TEST_ID),
                properties.get(SystemTestProperty.SYSTEM_TEST_VPC_ID),
                properties.get(SystemTestProperty.SYSTEM_TEST_JARS_BUCKET),
                bucketStack);
    }

    private static void createSystemTestCluster(Construct scope, SystemTestPropertyValues properties,
                                                SystemTestPropertySetter propertySetter,
                                                String deploymentId, String vpcId, String jarsBucketName,
                                                SystemTestBucketStack bucketStack) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(vpcId)
                .build();
        IVpc vpc = Vpc.fromLookup(scope, "VPC2", vpcLookupOptions);
        IBucket jarsBucket = Bucket.fromBucketName(scope, "JarsBucket", jarsBucketName);

        // ECS cluster for tasks to write data
        String clusterName = generateSystemTestClusterName(deploymentId);
        Cluster cluster = Cluster.Builder
                .create(scope, "SystemTestCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        propertySetter.set(SYSTEM_TEST_CLUSTER_NAME, cluster.getClusterName());
        CfnOutputProps writeClusterOutputProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(scope, "systemTestClusterName", writeClusterOutputProps);

        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder
                .create(scope, "TaskDefinition")
                .family(deploymentId + "SystemTestTaskFamily")
                .cpu(properties.getInt(SYSTEM_TEST_TASK_CPU))
                .memoryLimitMiB(properties.getInt(SYSTEM_TEST_TASK_MEMORY))
                .taskRole(Role.Builder.create(scope, "SystemTestTaskRole")
                        .roleName(Utils.truncateTo64Characters(String.join("-", "sleeper",
                                deploymentId.toLowerCase(Locale.ROOT), "system-test-writer")))
                        .build())
                .build();
        propertySetter.set(WRITE_DATA_TASK_DEFINITION_FAMILY, taskDefinition.getFamily());
        propertySetter.set(WRITE_DATA_ROLE_NAME, taskDefinition.getTaskRole().getRoleName());
        CfnOutputProps taskDefinitionFamilyOutputProps = new CfnOutputProps.Builder()
                .value(taskDefinition.getFamily())
                .build();
        new CfnOutput(scope, "TaskDefinitionFamily", taskDefinitionFamilyOutputProps);

        IRepository repository = Repository.fromRepositoryName(scope, "ECRRepo", properties.get(SYSTEM_TEST_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, SleeperVersion.getVersion());

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(createECSContainerLogDriver(scope, deploymentId))
                .build();
        taskDefinition.addContainer(SystemTestConstants.SYSTEM_TEST_CONTAINER, containerDefinitionOptions);

        bucketStack.getBucket().grantReadWrite(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
    }

    private static LogDriver createECSContainerLogDriver(Construct scope, String deploymentId) {
        String driverId = "SystemTestTasks";
        AwsLogDriverProps logDriverProps = AwsLogDriverProps.builder()
                .streamPrefix(deploymentId + "-" + driverId)
                .logGroup(LogGroup.Builder.create(scope, driverId)
                        .logGroupName(deploymentId + "-" + driverId)
                        .retention(getRetentionDays(30))
                        .build())
                .build();
        return LogDriver.awsLogs(logDriverProps);
    }
}
