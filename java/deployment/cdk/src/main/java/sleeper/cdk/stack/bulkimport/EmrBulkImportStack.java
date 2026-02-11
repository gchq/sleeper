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
package sleeper.cdk.stack.bulkimport;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.cdk.artefacts.SleeperJarsInBucket;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;

/**
 * Deploys resources to perform bulk import jobs on EMR, with a cluster created per job. A message arriving on a queue
 * triggers a lambda. That lambda creates an EMR cluster that executes the bulk import job and then terminates.
 */
public class EmrBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;

    public EmrBulkImportStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            SleeperJarsInBucket jars,
            BulkImportBucketStack importBucketStack,
            CommonEmrBulkImportStack commonEmrStack,
            SleeperCoreStacks coreStacks) {
        super(scope, id);

        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(
                this, BulkImportPlatform.NonPersistentEMR, instanceProperties, coreStacks);
        bulkImportJobQueue = commonHelper.createJobQueue(
                BULK_IMPORT_EMR_JOB_QUEUE_URL, BULK_IMPORT_EMR_JOB_QUEUE_ARN);
        IFunction jobStarter = commonHelper.createJobStarterFunction(
                bulkImportJobQueue, jars, importBucketStack.getImportBucket(),
                LogGroupRef.BULK_IMPORT_EMR_NON_PERSISTENT_START, commonEmrStack);

        configureJobStarterFunction(instanceProperties, jobStarter);
        Utils.addTags(this, instanceProperties);
    }

    private static void configureJobStarterFunction(
            InstanceProperties instanceProperties, IFunction bulkImportJobStarter) {
        Map<String, Map<String, String>> conditions = new HashMap<>();
        Map<String, String> tagKeyCondition = new HashMap<>();
        instanceProperties.getTags().forEach((key, value) -> tagKeyCondition.put("elasticmapreduce:RequestTag/" + key, value));

        conditions.put("StringEquals", tagKeyCondition);

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(List.of(
                        "elasticmapreduce:RunJobFlow",
                        "elasticmapreduce:AddTags"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .conditions(conditions)
                .build());

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("CreateCleanupRole")
                .actions(List.of("iam:CreateServiceLinkedRole", "iam:PutRolePolicy"))
                .resources(List.of("arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"))
                .conditions(Map.of("StringLike", Map.of("iam:AWSServiceName",
                        List.of("elasticmapreduce.amazonaws.com",
                                "elasticmapreduce.amazonaws.com.cn"))))
                .build());
    }

    public Queue getBulkImportJobQueue() {
        return bulkImportJobQueue;
    }
}
