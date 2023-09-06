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
package sleeper.ingest.status.store.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Arrays;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_STATUS_STORE_ENABLED;
import static sleeper.dynamodb.tools.DynamoDBUtils.configureTimeToLive;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.EXPIRY_DATE;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.JOB_ID;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.UPDATE_TIME;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore.jobStatusTableName;

public class DynamoDBIngestJobStatusStoreCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusStoreCreator.class);

    private DynamoDBIngestJobStatusStoreCreator() {
    }

    public static void create(InstanceProperties properties, AmazonDynamoDB dynamoDB) {
        if (!properties.getBoolean(INGEST_STATUS_STORE_ENABLED)) {
            return;
        }
        String tableName = jobStatusTableName(properties.get(ID));
        initialiseTable(dynamoDB, tableName,
                Arrays.asList(
                        new AttributeDefinition(JOB_ID, ScalarAttributeType.S),
                        new AttributeDefinition(UPDATE_TIME, ScalarAttributeType.N)),
                Arrays.asList(
                        new KeySchemaElement(JOB_ID, KeyType.HASH),
                        new KeySchemaElement(UPDATE_TIME, KeyType.RANGE)));
        configureTimeToLive(dynamoDB, tableName, EXPIRY_DATE);
    }

    public static void tearDown(InstanceProperties properties, AmazonDynamoDB dynamoDBClient) {
        if (!properties.getBoolean(INGEST_STATUS_STORE_ENABLED)) {
            return;
        }
        String tableName = jobStatusTableName(properties.get(ID));
        LOGGER.info("Deleting table: {}", tableName);
        dynamoDBClient.deleteTable(tableName);
    }
}
