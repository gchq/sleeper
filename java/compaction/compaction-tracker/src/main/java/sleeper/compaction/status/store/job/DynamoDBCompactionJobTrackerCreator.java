/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.compaction.status.store.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.compaction.status.store.job.DynamoDBCompactionJobTracker.EXPIRY_DATE;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobTracker.JOB_ID;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobTracker.JOB_ID_AND_UPDATE;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobTracker.TABLE_ID;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobTracker.jobLookupTableName;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobTracker.jobUpdatesTableName;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.dynamodb.tools.DynamoDBUtils.configureTimeToLive;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;

public class DynamoDBCompactionJobTrackerCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobTrackerCreator.class);

    private DynamoDBCompactionJobTrackerCreator() {
    }

    public static void create(InstanceProperties properties, AmazonDynamoDB dynamoDB) {
        if (!properties.getBoolean(COMPACTION_TRACKER_ENABLED)) {
            return;
        }
        String updatesTableName = jobUpdatesTableName(properties.get(ID));
        String jobsTableName = jobLookupTableName(properties.get(ID));
        initialiseTable(dynamoDB, properties.getTags(), new CreateTableRequest()
                .withTableName(updatesTableName)
                .withAttributeDefinitions(
                        new AttributeDefinition(TABLE_ID, ScalarAttributeType.S),
                        new AttributeDefinition(JOB_ID_AND_UPDATE, ScalarAttributeType.S))
                .withKeySchema(
                        new KeySchemaElement(TABLE_ID, KeyType.HASH),
                        new KeySchemaElement(JOB_ID_AND_UPDATE, KeyType.RANGE)));
        initialiseTable(dynamoDB, properties.getTags(), new CreateTableRequest()
                .withTableName(jobsTableName)
                .withAttributeDefinitions(new AttributeDefinition(JOB_ID, ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement(JOB_ID, KeyType.HASH)));
        configureTimeToLive(dynamoDB, updatesTableName, EXPIRY_DATE);
        configureTimeToLive(dynamoDB, jobsTableName, EXPIRY_DATE);
    }

    public static void tearDown(InstanceProperties properties, AmazonDynamoDB dynamoDBClient) {
        if (!properties.getBoolean(COMPACTION_TRACKER_ENABLED)) {
            return;
        }
        String jobsTableName = jobLookupTableName(properties.get(ID));
        String updatesTableName = jobUpdatesTableName(properties.get(ID));
        LOGGER.info("Deleting table: {}", jobsTableName);
        dynamoDBClient.deleteTable(jobsTableName);
        LOGGER.info("Deleting table: {}", updatesTableName);
        dynamoDBClient.deleteTable(updatesTableName);
    }
}
