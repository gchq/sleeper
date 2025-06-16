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
package sleeper.clients.deploy.localstack.stack;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.localstack.TearDownBucket;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.util.Locale;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ONLINE_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class TableDockerStack {
    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDB;

    private TableDockerStack(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoDB) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        this.dynamoDB = dynamoDB;
    }

    public static TableDockerStack from(
            InstanceProperties instanceProperties,
            S3Client s3Client, DynamoDbClient dynamoDB) {
        return new TableDockerStack(instanceProperties, s3Client, dynamoDB);
    }

    public void deploy() {
        String instanceId = instanceProperties.get(ID).toLowerCase(Locale.ROOT);
        String dataBucket = String.join("-", "sleeper", instanceId, "table-data");
        instanceProperties.set(DATA_BUCKET, dataBucket);
        s3Client.createBucket(request -> request.bucket(dataBucket));
        instanceProperties.set(TABLE_NAME_INDEX_DYNAMO_TABLENAME, String.join("-", "sleeper", instanceId, "table-index-by-name"));
        instanceProperties.set(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME, String.join("-", "sleeper", instanceId, "table-index-online-by-name"));
        instanceProperties.set(TABLE_ID_INDEX_DYNAMO_TABLENAME, String.join("-", "sleeper", instanceId, "table-index-by-id"));
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        instanceProperties.set(TRANSACTION_LOG_FILES_TABLENAME, String.join("-", "sleeper", instanceId, "-ftl"));
        instanceProperties.set(TRANSACTION_LOG_PARTITIONS_TABLENAME, String.join("-", "sleeper", instanceId, "-ptl"));
        instanceProperties.set(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME, String.join("-", "sleeper", instanceId, "-tlas"));
        instanceProperties.set(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME, String.join("-", "sleeper", instanceId, "-tlls"));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();
    }

    public void tearDown() {
        dynamoDB.deleteTable(request -> request.tableName(instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME)));
        dynamoDB.deleteTable(request -> request.tableName(instanceProperties.get(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME)));
        dynamoDB.deleteTable(request -> request.tableName(instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME)));
        dynamoDB.deleteTable(request -> request.tableName(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME)));
        dynamoDB.deleteTable(request -> request.tableName(instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME)));
        dynamoDB.deleteTable(request -> request.tableName(instanceProperties.get(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME)));
        dynamoDB.deleteTable(request -> request.tableName(instanceProperties.get(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME)));
        TearDownBucket.emptyAndDelete(s3Client, instanceProperties.get(DATA_BUCKET));
    }

}
