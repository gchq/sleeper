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
package sleeper.cdk.stack;

import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.s3.Bucket;
import software.constructs.Construct;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.s3.S3StateStore;

import java.util.Locale;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class S3StateStoreStack implements StateStoreStack {
    private final Table revisionTable;
    private final Bucket dataBucket;

    public S3StateStoreStack(Construct scope,
                             Bucket dataBucket,
                             InstanceProperties instanceProperties,
                             TableProperties tableProperties,
                             Provider tablesProvider) {
        this.dataBucket = dataBucket;

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        // Dynamo table to store latest revision version
        Attribute partitionKeyRevisionTable = Attribute.builder()
                .name(S3StateStore.REVISION_ID_KEY)
                .type(AttributeType.STRING)
                .build();

        this.revisionTable = Table.Builder
                .create(scope, "DynamoDBRevisionTable")
                .tableName(String.join("-", "sleeper", instanceProperties.get(ID), "table",
                        tableProperties.get(TABLE_NAME), "revisions").toLowerCase(Locale.ROOT))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyRevisionTable)
                .pointInTimeRecovery(tableProperties.getBoolean(S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY))
                .build();
        tableProperties.set(REVISION_TABLENAME, this.revisionTable.getTableName());

        this.revisionTable.grantReadWriteData(tablesProvider.getOnEventHandler());
    }

    @Override
    public void grantReadActiveFileMetadata(IGrantable grantee) {
        grantRead(grantee);
    }

    @Override
    public void grantReadWriteActiveFileMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    @Override
    public void grantReadWriteReadyForGCFileMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    @Override
    public void grantWriteReadyForGCFileMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    @Override
    public void grantReadPartitionMetadata(IGrantable grantee) {
        grantRead(grantee);
    }

    @Override
    public void grantReadWritePartitionMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    private void grantReadWrite(IGrantable grantee) {
        revisionTable.grantReadWriteData(grantee);
        dataBucket.grantReadWrite(grantee); // TODO Only needs access to keys starting with 'statestore'
    }

    private void grantRead(IGrantable grantee) {
        revisionTable.grantReadData(grantee);
        dataBucket.grantRead(grantee); // TODO Only needs access to keys starting with 'statestore'
    }
}
