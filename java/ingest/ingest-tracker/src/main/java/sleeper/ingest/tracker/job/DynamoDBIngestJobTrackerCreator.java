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
package sleeper.ingest.tracker.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;
import static sleeper.dynamodb.tools.DynamoDBUtils.configureTimeToLive;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.EXPIRY_DATE;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.JOB_ID;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.JOB_ID_AND_UPDATE;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.JOB_LAST_VALIDATION_RESULT;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.TABLE_ID;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.VALIDATION_INDEX;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.jobLookupTableName;
import static sleeper.ingest.tracker.job.DynamoDBIngestJobTracker.jobUpdatesTableName;

public class DynamoDBIngestJobTrackerCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobTrackerCreator.class);

    private DynamoDBIngestJobTrackerCreator() {
    }

    public static void create(InstanceProperties properties, DynamoDbClient dynamoDB) {
        if (!properties.getBoolean(INGEST_TRACKER_ENABLED)) {
            return;
        }
        String updatesTableName = jobUpdatesTableName(properties.get(ID));
        String jobsTableName = jobLookupTableName(properties.get(ID));
        initialiseTable(dynamoDB, properties.getTags(), CreateTableRequest.builder()
                .tableName(updatesTableName)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(TABLE_ID).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(JOB_ID_AND_UPDATE).attributeType(ScalarAttributeType.S).build())
                .keySchema(
                        KeySchemaElement.builder().attributeName(TABLE_ID).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(JOB_ID_AND_UPDATE).keyType(KeyType.RANGE).build())
                .build());
        initialiseTable(dynamoDB, properties.getTags(), CreateTableRequest.builder()
                .tableName(jobsTableName)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(JOB_ID).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(JOB_LAST_VALIDATION_RESULT).attributeType(ScalarAttributeType.S).build())
                .keySchema(KeySchemaElement.builder().attributeName(JOB_ID).keyType(KeyType.HASH).build())
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(VALIDATION_INDEX)
                        .keySchema(KeySchemaElement.builder().attributeName(JOB_LAST_VALIDATION_RESULT).keyType(KeyType.HASH).build())
                        .projection(Projection.builder()
                                .projectionType(ProjectionType.INCLUDE)
                                .nonKeyAttributes(TABLE_ID)
                                .build())
                        .build())
                .build());
        configureTimeToLive(dynamoDB, updatesTableName, EXPIRY_DATE);
        configureTimeToLive(dynamoDB, jobsTableName, EXPIRY_DATE);
    }

    public static void tearDown(InstanceProperties properties, DynamoDbClient dynamoDBClient) {
        if (!properties.getBoolean(INGEST_TRACKER_ENABLED)) {
            return;
        }
        String jobsTableName = jobLookupTableName(properties.get(ID));
        String updatesTableName = jobUpdatesTableName(properties.get(ID));
        LOGGER.info("Deleting table: {}", jobsTableName);
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(jobsTableName).build());
        LOGGER.info("Deleting table: {}", updatesTableName);
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(updatesTableName).build());
    }
}
