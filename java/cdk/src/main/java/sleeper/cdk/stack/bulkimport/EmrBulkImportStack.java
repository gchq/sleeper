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
package sleeper.cdk.stack.bulkimport;

import com.google.common.collect.Lists;
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.emrserverless.CfnApplication;
import software.amazon.awscdk.services.emrserverless.CfnApplication.ImageConfigurationInputProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplication.NetworkConfigurationProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplicationProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.IngestStatusStoreResources;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.configuration.properties.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_ENABLED;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_RELEASE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;

/**
 * An {@link EmrBulkImportStack} creates an SQS queue that bulk import jobs can
 * be sent to. A message arriving on this queue triggers a lambda. That lambda
 * creates an EMR cluster that executes the bulk import job and then terminates.
 */
public class EmrBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;
    private final IntUnaryOperator randomSubnet;

    public EmrBulkImportStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            BulkImportBucketStack importBucketStack,
            CommonEmrBulkImportStack commonEmrStack,
            TopicStack errorsTopicStack,
            List<StateStoreStack> stateStoreStacks,
            IngestStatusStoreResources statusStoreResources) {
        super(scope, id);

        String instanceName = "NonPersistentEMR";
        if (instanceProperties.getBoolean(BULK_IMPORT_EMR_SERVERLESS_ENABLED)) {
            createEmrServerlessApplication(scope, instanceProperties);
            instanceName = "EMRServerless";
        }

        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(
                this, instanceName, instanceProperties, statusStoreResources);
        bulkImportJobQueue = commonHelper.createJobQueue(BULK_IMPORT_EMR_JOB_QUEUE_URL, errorsTopicStack.getTopic());
        IFunction jobStarter = commonHelper.createJobStarterFunction(
                instanceName, bulkImportJobQueue, jars, importBucketStack.getImportBucket(), commonEmrStack);
        stateStoreStacks.forEach(sss -> sss.grantReadPartitionMetadata(jobStarter));

        configureJobStarterFunction(instanceProperties, jobStarter);
        Utils.addStackTagIfSet(this, instanceProperties);
        randomSubnet = new Random()::nextInt;
    }

    private static void configureJobStarterFunction(
            InstanceProperties instanceProperties, IFunction bulkImportJobStarter) {
        Map<String, Map<String, String>> conditions = new HashMap<>();
        Map<String, String> tagKeyCondition = new HashMap<>();
        instanceProperties.getTags().forEach((key, value) -> tagKeyCondition.put("elasticmapreduce:RequestTag/" + key, value));

        conditions.put("StringEquals", tagKeyCondition);

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("elasticmapreduce:RunJobFlow"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .conditions(conditions)
                .build());
    }

    public Queue getBulkImportJobQueue() {
        return bulkImportJobQueue;
    }

    public void createEmrServerlessApplication(Construct scope, InstanceProperties instanceProperties) {
        Properties properties = instanceProperties.getProperties();

        CfnApplicationProps props = CfnApplicationProps.builder()
                .name(String.join("-", "sleeper", "emr", "serverless"))
                .releaseLabel(properties.get(BULK_IMPORT_EMR_SERVERLESS_RELEASE).toString())
                .architecture(properties.get(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE).toString())
                .type(properties.get(BULK_IMPORT_EMR_SERVERLESS_TYPE).toString())
                .imageConfiguration(ImageConfigurationInputProperty.builder()
                        .imageUri(properties.get(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO).toString())
                        .build()
                )
                .networkConfiguration(NetworkConfigurationProperty.builder()
                        .securityGroupIds(List.of(properties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP).toString()))
                        .subnetIds(List.of(randomSubnet(instanceProperties)))
                        .build()
                )
                .tags(List.of(new CfnTag.Builder()
                        .key("DeploymentStack")
                        .value("BulkImportServerlessEMR")
                        .build()))
                .build();

        CfnApplication emrsCluster = new CfnApplication(scope, getArtifactId(), props);
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME, emrsCluster.getName());
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID, emrsCluster.getAttrApplicationId());
    }

    private String randomSubnet(InstanceProperties instanceProperties) {
        List<String> subnets = instanceProperties.getList(SUBNETS);
        return subnets.get(randomSubnet.applyAsInt(subnets.size()));
    }
}
