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

package sleeper.cdk.stack.ingest;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.GlobalSecondaryIndexProps;
import software.amazon.awscdk.services.dynamodb.PointInTimeRecoverySpecification;
import software.amazon.awscdk.services.dynamodb.ProjectionType;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.cdk.stack.core.ManagedPoliciesStack;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.tracker.job.DynamoDBIngestJobTracker;
import sleeper.ingest.tracker.task.DynamoDBIngestTaskStatusFormat;
import sleeper.ingest.tracker.task.DynamoDBIngestTaskTracker;

import java.util.List;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class IngestTrackerStack extends NestedStack implements IngestTrackerResources {
    private final Table updatesTable;
    private final Table jobsTable;
    private final Table tasksTable;

    public IngestTrackerStack(
            Construct scope, String id, InstanceProperties instanceProperties, ManagedPoliciesStack policiesStack) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        updatesTable = Table.Builder
                .create(this, "DynamoDBIngestJobUpdatesTable")
                .tableName(DynamoDBIngestJobTracker.jobUpdatesTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBIngestJobTracker.TABLE_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBIngestJobTracker.JOB_ID_AND_UPDATE)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(DynamoDBIngestJobTracker.EXPIRY_DATE)
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(false)
                        .build())
                .build();

        jobsTable = Table.Builder
                .create(this, "DynamoDBIngestJobLookupTable")
                .tableName(DynamoDBIngestJobTracker.jobLookupTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBIngestJobTracker.JOB_ID)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(DynamoDBIngestJobTracker.EXPIRY_DATE)
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(false)
                        .build())
                .build();

        jobsTable.addGlobalSecondaryIndex(GlobalSecondaryIndexProps.builder()
                .indexName(DynamoDBIngestJobTracker.VALIDATION_INDEX)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBIngestJobTracker.JOB_LAST_VALIDATION_RESULT)
                        .type(AttributeType.STRING)
                        .build())
                .projectionType(ProjectionType.INCLUDE)
                .nonKeyAttributes(List.of(DynamoDBIngestJobTracker.TABLE_ID))
                .build());

        tasksTable = Table.Builder
                .create(this, "DynamoDBIngestTaskStatusTable")
                .tableName(DynamoDBIngestTaskTracker.taskStatusTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBIngestTaskStatusFormat.TASK_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBIngestTaskStatusFormat.UPDATE_TIME)
                        .type(AttributeType.NUMBER)
                        .build())
                .timeToLiveAttribute(DynamoDBIngestTaskStatusFormat.EXPIRY_DATE)
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(false)
                        .build())
                .build();

        grantWriteJobEvent(policiesStack.getDirectIngestPolicyForGrants());
        updatesTable.grantReadData(policiesStack.getReportingPolicyForGrants());
        jobsTable.grantReadData(policiesStack.getReportingPolicyForGrants());
        tasksTable.grantReadData(policiesStack.getReportingPolicyForGrants());
        Utils.addTags(this, instanceProperties);
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
