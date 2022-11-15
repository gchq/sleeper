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
import sleeper.ingest.status.DynamoDBIngestJobStatusFormat;
import sleeper.ingest.status.DynamoDBIngestJobStatusStore;
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
    private final Table table;

    public DynamoDBIngestStatusStoreStack(Construct scope, InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        this.table = Table.Builder
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
                .pointInTimeRecovery(false)
                .build();
    }

    @Override
    public void grantWriteJobEvent(IGrantable grantee) {
        table.grantWriteData(grantee);
    }
}
