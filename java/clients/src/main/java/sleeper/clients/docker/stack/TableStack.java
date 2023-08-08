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

package sleeper.clients.docker.stack;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.table.job.TableCreator;

import java.io.IOException;

import static sleeper.clients.docker.Utils.tearDownBucket;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;

public class TableStack {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDB;

    private TableStack(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
        s3Client = builder.s3Client;
        dynamoDB = builder.dynamoDB;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TableStack from(InstanceProperties instanceProperties, TableProperties tableProperties,
                                  AmazonS3 s3Client, AmazonDynamoDB dynamoDB) {
        return builder().instanceProperties(instanceProperties)
                .tableProperties(tableProperties)
                .s3Client(s3Client).dynamoDB(dynamoDB)
                .build();
    }

    public void deploy() throws IOException, StateStoreException {
        tableProperties.saveToS3(s3Client);
        s3Client.createBucket(tableProperties.get(DATA_BUCKET));

        new TableCreator(s3Client, dynamoDB, instanceProperties).createTable(tableProperties);
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDB).create();
        stateStore.initialise();
    }

    public void tearDown() {
        dynamoDB.deleteTable(tableProperties.get(ACTIVE_FILEINFO_TABLENAME));
        dynamoDB.deleteTable(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME));
        dynamoDB.deleteTable(tableProperties.get(PARTITION_TABLENAME));
        tearDownBucket(s3Client, tableProperties.get(DATA_BUCKET));
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties;
        private AmazonS3 s3Client;
        private AmazonDynamoDB dynamoDB;

        public Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder s3Client(AmazonS3 s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public TableStack build() {
            return new TableStack(this);
        }
    }
}
