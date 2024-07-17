/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.cdk.stack;

import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStore;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusFormat;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public final class DynamoDBCompactionStatusStoreResources implements CompactionStatusStoreResources {

    private final Table updatesTable;
    private final Table jobsTable;
    private final Table tasksTable;

    public DynamoDBCompactionStatusStoreResources(
            Construct scope, InstanceProperties instanceProperties, ManagedPoliciesStack policiesStack) {
        String instanceId = instanceProperties.get(ID);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        updatesTable = Table.Builder
                .create(scope, "DynamoDBCompactionJobUpdatesTable")
                .tableName(DynamoDBCompactionJobStatusStore.jobUpdatesTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBCompactionJobStatusStore.TABLE_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBCompactionJobStatusStore.JOB_ID_AND_UPDATE)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(DynamoDBCompactionJobStatusStore.EXPIRY_DATE)
                .pointInTimeRecovery(false)
                .build();

        jobsTable = Table.Builder
                .create(scope, "DynamoDBCompactionJobLookupTable")
                .tableName(DynamoDBCompactionJobStatusStore.jobLookupTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBCompactionJobStatusStore.JOB_ID)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(DynamoDBCompactionJobStatusStore.EXPIRY_DATE)
                .pointInTimeRecovery(false)
                .build();

        tasksTable = Table.Builder
                .create(scope, "DynamoDBCompactionTaskStatusTable")
                .tableName(DynamoDBCompactionTaskStatusStore.taskStatusTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBCompactionTaskStatusFormat.TASK_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBCompactionTaskStatusFormat.UPDATE_TIME)
                        .type(AttributeType.NUMBER)
                        .build())
                .timeToLiveAttribute(DynamoDBCompactionTaskStatusFormat.EXPIRY_DATE)
                .pointInTimeRecovery(false)
                .build();

        grantWriteJobEvent(policiesStack.getInvokeCompactionPolicyForGrants());
        updatesTable.grantReadData(policiesStack.getReportingPolicyForGrants());
        jobsTable.grantReadData(policiesStack.getReportingPolicyForGrants());
        tasksTable.grantReadData(policiesStack.getReportingPolicyForGrants());
    }

    @Override
    public void grantWriteJobEvent(IGrantable grantee) {
        updatesTable.grantWriteData(grantee);
        jobsTable.grantWriteData(grantee);
    }

    @Override
    public void grantWriteTaskEvent(IGrantable grantee) {
        tasksTable.grantWriteData(grantee);
    }
}
