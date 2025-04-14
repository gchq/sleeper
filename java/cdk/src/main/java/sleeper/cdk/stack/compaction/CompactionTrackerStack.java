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
package sleeper.cdk.stack.compaction;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.PointInTimeRecoverySpecification;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.cdk.stack.core.ManagedPoliciesStack;
import sleeper.compaction.tracker.job.DynamoDBCompactionJobTracker;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskStatusFormat;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskTracker;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class CompactionTrackerStack extends NestedStack implements CompactionTrackerResources {
    private final Table updatesTable;
    private final Table jobsTable;
    private final Table tasksTable;

    public CompactionTrackerStack(
            Construct scope, String id, InstanceProperties instanceProperties, ManagedPoliciesStack policiesStack) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        updatesTable = Table.Builder
                .create(this, "DynamoDBCompactionJobUpdatesTable")
                .tableName(DynamoDBCompactionJobTracker.jobUpdatesTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBCompactionJobTracker.TABLE_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBCompactionJobTracker.JOB_ID_AND_UPDATE)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(DynamoDBCompactionJobTracker.EXPIRY_DATE)
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(false)
                        .build())
                .build();

        jobsTable = Table.Builder
                .create(this, "DynamoDBCompactionJobLookupTable")
                .tableName(DynamoDBCompactionJobTracker.jobLookupTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBCompactionJobTracker.JOB_ID)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(DynamoDBCompactionJobTracker.EXPIRY_DATE)
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(false)
                        .build())
                .build();

        tasksTable = Table.Builder
                .create(this, "DynamoDBCompactionTaskStatusTable")
                .tableName(DynamoDBCompactionTaskTracker.taskStatusTableName(instanceId))
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
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(false)
                        .build())
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
