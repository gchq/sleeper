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
import software.amazon.awscdk.NestedStack;
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

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;

/**
 * An {@link EmrBulkImportStack} creates an SQS queue that bulk import jobs can
 * be sent to. A message arriving on this queue triggers a lambda. That lambda
 * creates an EMR cluster that executes the bulk import job and then terminates.
 */
public class EmrBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;

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

        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(
                this, "NonPersistentEMR", instanceProperties, statusStoreResources);
        bulkImportJobQueue = commonHelper.createJobQueue(BULK_IMPORT_EMR_JOB_QUEUE_URL, errorsTopicStack.getTopic());
        IFunction jobStarter = commonHelper.createJobStarterFunction(
                "NonPersistentEMR", bulkImportJobQueue, jars, importBucketStack.getImportBucket(), commonEmrStack);
        stateStoreStacks.forEach(sss -> sss.grantReadPartitionMetadata(jobStarter));

        configureJobStarterFunction(instanceProperties, jobStarter);
        Utils.addStackTagIfSet(this, instanceProperties);
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
}
