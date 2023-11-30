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
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.util.Locale;

import static sleeper.clients.docker.Utils.tearDownBucket;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class TableDockerStack implements DockerStack {
    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDB;

    private TableDockerStack(Builder builder) {
        instanceProperties = builder.instanceProperties;
        s3Client = builder.s3Client;
        dynamoDB = builder.dynamoDB;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TableDockerStack from(InstanceProperties instanceProperties,
                                        AmazonS3 s3Client, AmazonDynamoDB dynamoDB) {
        return builder().instanceProperties(instanceProperties)
                .s3Client(s3Client).dynamoDB(dynamoDB)
                .build();
    }

    public void deploy() {
        String instanceId = instanceProperties.get(ID).toLowerCase(Locale.ROOT);
        String dataBucket = String.join("-", "sleeper", instanceId, "table-data");
        instanceProperties.set(DATA_BUCKET, dataBucket);
        s3Client.createBucket(dataBucket);
        instanceProperties.set(TABLE_NAME_INDEX_DYNAMO_TABLENAME, String.join("-", "sleeper", instanceId, "table-index-by-name"));
        instanceProperties.set(TABLE_ID_INDEX_DYNAMO_TABLENAME, String.join("-", "sleeper", instanceId, "table-index-by-id"));
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        instanceProperties.set(ACTIVE_FILEINFO_TABLENAME, String.join("-", "sleeper", instanceId, "active-files"));
        instanceProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, String.join("-", "sleeper", instanceId, "gc-files"));
        instanceProperties.set(FILE_REFERENCE_COUNT_TABLENAME, String.join("-", "sleeper", instanceId, "file-ref-count"));
        instanceProperties.set(PARTITION_TABLENAME, String.join("-", "sleeper", instanceId, "partitions"));
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDB).create();
        instanceProperties.set(REVISION_TABLENAME, String.join("-", "sleeper", instanceId, "rv"));
        new S3StateStoreCreator(instanceProperties, dynamoDB).create();
    }

    public void tearDown() {
        dynamoDB.deleteTable(instanceProperties.get(ACTIVE_FILEINFO_TABLENAME));
        dynamoDB.deleteTable(instanceProperties.get(READY_FOR_GC_FILEINFO_TABLENAME));
        dynamoDB.deleteTable(instanceProperties.get(PARTITION_TABLENAME));
        dynamoDB.deleteTable(instanceProperties.get(REVISION_TABLENAME));
        tearDownBucket(s3Client, instanceProperties.get(DATA_BUCKET));
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private AmazonS3 s3Client;
        private AmazonDynamoDB dynamoDB;

        public Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
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

        public TableDockerStack build() {
            return new TableDockerStack(this);
        }
    }
}
