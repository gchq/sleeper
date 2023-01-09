/*
 * Copyright 2023 Crown Copyright
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.constructs.Construct;

import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;

/**
 * An {@link EmrBulkImportStack} creates an SQS queue that bulk import jobs can
 * be sent to. A message arriving on this queue triggers a lambda. That lambda
 * creates an EMR cluster that executes the bulk import job and then terminates.
 */
public class EmrBulkImportStack extends AbstractEmrBulkImportStack {

    public EmrBulkImportStack(
            Construct scope,
            String id,
            List<IBucket> dataBuckets,
            List<StateStoreStack> stateStoreStacks,
            InstanceProperties instanceProperties,
            ITopic errorsTopic) {
        super(scope, id, "NonPersistentEMR", "NonPersistentEMR",
                BULK_IMPORT_EMR_JOB_QUEUE_URL, dataBuckets, stateStoreStacks, instanceProperties, errorsTopic);
    }

    @Override
    protected void createBulkImportJobStarterFunction() {
        super.createBulkImportJobStarterFunction();

        Map<String, Map<String, String>> conditions = new HashMap<>();
        Map<String, String> tagKeyCondition = new HashMap<>();

        instanceProperties.getTags().entrySet().stream().forEach(entry -> {
            tagKeyCondition.put("elasticmapreduce:RequestTag/" + entry.getKey(), entry.getValue());
        });

        conditions.put("StringEquals", tagKeyCondition);

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("elasticmapreduce:RunJobFlow"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .conditions(conditions)
                .build());

        String arnPrefix = "arn:aws:iam::" + instanceProperties.get(UserDefinedInstanceProperty.ACCOUNT) + ":role/";

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Lists.newArrayList("iam:PassRole"))
                .resources(Lists.newArrayList(
                        arnPrefix + instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME),
                        arnPrefix + instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME)
                ))
                .build());

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("CreateCleanupRole")
                .actions(Lists.newArrayList("iam:CreateServiceLinkedRole", "iam:PutRolePolicy"))
                .resources(Lists.newArrayList("arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"))
                .conditions(ImmutableMap.of("StringLike", ImmutableMap.of("iam:AWSServiceName",
                        Lists.newArrayList("elasticmapreduce.amazonaws.com",
                                "elasticmapreduce.amazonaws.com.cn"))))
                .build());
    }
}
