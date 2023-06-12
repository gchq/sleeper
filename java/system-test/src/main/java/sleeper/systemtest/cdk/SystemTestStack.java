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
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.SystemTestProperty;

import java.util.List;
import java.util.Locale;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_TASK_CPU;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_TASK_MEMORY;
import static sleeper.systemtest.SystemTestProperty.WRITE_DATA_TASK_DEFINITION_FAMILY;

/**
 * A {@link NestedStack} to deploy the system test components.
 */
public class SystemTestStack extends NestedStack {
    public static final String SYSTEM_TEST_CLUSTER_NAME = "systemTestClusterName";
    public static final String SYSTEM_TEST_TASK_DEFINITION_FAMILY = "systemTestTaskDefinitionFamily";
    public static final String SYSTEM_TEST_CONTAINER = "SystemTestContainer";

    public SystemTestStack(Construct scope,
                           String id,
                           List<IBucket> dataBuckets,
                           List<StateStoreStack> stateStoreStacks,
                           SystemTestProperties systemTestProperties,
                           Queue ingestJobQueue,
                           Queue emrBulkImportJobQueue) {
        super(scope, id);

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", systemTestProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", systemTestProperties.get(JARS_BUCKET));

        // ECS cluster for tasks to write data
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(systemTestProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC2", vpcLookupOptions);
        String clusterName = generateSystemTestClusterName(systemTestProperties.get(ID));
        Cluster cluster = Cluster.Builder
                .create(this, "SystemTestCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        systemTestProperties.set(SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME, cluster.getClusterName());
        CfnOutputProps writeClusterOutputProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, SYSTEM_TEST_CLUSTER_NAME, writeClusterOutputProps);

        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder
                .create(this, "SystemTestTaskDefinition")
                .family(systemTestProperties.get(ID) + "SystemTestTaskFamily")
                .cpu(systemTestProperties.getInt(SYSTEM_TEST_TASK_CPU))
                .memoryLimitMiB(systemTestProperties.getInt(SYSTEM_TEST_TASK_MEMORY))
                .build();
        systemTestProperties.set(WRITE_DATA_TASK_DEFINITION_FAMILY, taskDefinition.getFamily());
        CfnOutputProps taskDefinitionFamilyOutputProps = new CfnOutputProps.Builder()
                .value(taskDefinition.getFamily())
                .build();
        new CfnOutput(this, SYSTEM_TEST_TASK_DEFINITION_FAMILY, taskDefinitionFamilyOutputProps);

        IRepository repository = Repository.fromRepositoryName(this, "ECR3", systemTestProperties.get(SYSTEM_TEST_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, systemTestProperties.get(VERSION));

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(Utils.createECSContainerLogDriver(this, systemTestProperties, "SystemTestTasks"))
                .environment(Utils.createDefaultEnvironment(systemTestProperties))
                .build();
        taskDefinition.addContainer(SYSTEM_TEST_CONTAINER, containerDefinitionOptions);

        configBucket.grantRead(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
        dataBuckets.forEach(bucket -> bucket.grantReadWrite(taskDefinition.getTaskRole()));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteActiveFileMetadata(taskDefinition.getTaskRole()));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadPartitionMetadata(taskDefinition.getTaskRole()));
        if (null != ingestJobQueue) {
            ingestJobQueue.grantSendMessages(taskDefinition.getTaskRole());
        }
        if (null != emrBulkImportJobQueue) {
            emrBulkImportJobQueue.grantSendMessages(taskDefinition.getTaskRole());
        }
    }

    public static String generateSystemTestClusterName(String instanceId) {
        return Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(Locale.ROOT), "system-test-cluster"));
    }
}
