/*
 * Copyright 2022 Crown Copyright
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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.ingest.job.status.DynamoDBIngestJobStatusFormat;
import sleeper.ingest.job.status.DynamoDBIngestJobStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusFormat;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStore;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class DynamoDBIngestStatusStoreStack implements IngestStatusStoreStack {
    private final Table jobsTable;
    private final Table tasksTable;

    public DynamoDBIngestStatusStoreStack(Construct scope, InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        this.jobsTable = Table.Builder
                .create(scope, "DynamoDBIngestJobStatusTable")
                .tableName(DynamoDBIngestJobStatusStore.jobStatusTableName(instanceId))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBIngestJobStatusFormat.JOB_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBIngestJobStatusFormat.UPDATE_TIME)
                        .type(AttributeType.NUMBER)
                        .build())
                .timeToLiveAttribute(DynamoDBIngestJobStatusFormat.EXPIRY_DATE)
                .pointInTimeRecovery(false)
                .build();
        this.tasksTable = Table.Builder
                .create(scope, "DynamoDBIngestTaskStatusTable")
                .tableName(DynamoDBIngestTaskStatusStore.taskStatusTableName(instanceId))
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
                .pointInTimeRecovery(false)
                .build();
    }

    @Override
    public void grantWriteJobEvent(IGrantable grantee) {
        jobsTable.grantWriteData(grantee);
    }

    @Override
    public void grantWriteTaskEvent(IGrantable grantee) {
        tasksTable.grantWriteData(grantee);
    }
}
