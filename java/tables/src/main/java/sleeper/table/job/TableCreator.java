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
package sleeper.table.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.Locale;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperties.TABLES_PREFIX;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * The TableCreator creates tables using the SDK. This is normally done using
 * CDK. This class is only intended to be used in tests. It creates a bucket,
 * StateStore and writes the properties to S3.
 * <p>
 * After creation, the StateStore will need to be initialised before it can be used.
 */
public class TableCreator {

    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final InstanceProperties instanceProperties;

    public TableCreator(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties) {
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
        this.instanceProperties = instanceProperties;
    }

    public void createTable(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        ensureTableDoesNotExist(tableName, instanceProperties.get(CONFIG_BUCKET));
        String instanceId = instanceProperties.get(ID);

        instanceProperties.set(ACTIVE_FILEINFO_TABLENAME, String.join("-",
                "sleeper", instanceId, "active-files").toLowerCase(Locale.ROOT));
        instanceProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, String.join("-",
                "sleeper", instanceId, "gc-files").toLowerCase(Locale.ROOT));
        instanceProperties.set(PARTITION_TABLENAME, String.join("-",
                "sleeper", instanceId, "partitions").toLowerCase(Locale.ROOT));

        // Create Dynamo tables
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
        instanceProperties.saveToS3(s3Client);
        tableProperties.saveToS3(s3Client);
    }

    private void ensureTableDoesNotExist(String name, String configBucket) {
        if (s3Client.doesObjectExist(configBucket, TABLES_PREFIX + "/" + name)) {
            throw new IllegalArgumentException("Table " + name + " already exists");
        }
    }
}
