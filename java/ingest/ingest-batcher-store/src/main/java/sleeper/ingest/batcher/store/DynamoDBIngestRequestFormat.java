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

package sleeper.ingest.batcher.store;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.batcher.FileIngestRequest;

import java.time.Duration;
import java.util.Map;

import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_TRACKING_TTL_MINUTES;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;

public class DynamoDBIngestRequestFormat {
    private DynamoDBIngestRequestFormat() {
    }

    public static final String FILE_PATH = "FilePath";
    public static final String FILE_SIZE = "FileSize";
    public static final String TABLE_NAME = "TableName";
    public static final String JOB_ID = "JobId";
    public static final String RECEIVED_TIME = "ReceivedTime";
    public static final String EXPIRY_TIME = "ExpiryTime";

    public static Map<String, AttributeValue> createRecord(
            TablePropertiesProvider tablePropertiesProvider, FileIngestRequest fileIngestRequest) {
        TableProperties properties = tablePropertiesProvider.getTableProperties(fileIngestRequest.getTableName());
        return new DynamoDBRecordBuilder()
                .string(FILE_PATH, fileIngestRequest.getPathToFile())
                .number(FILE_SIZE, fileIngestRequest.getFileSizeBytes())
                .string(TABLE_NAME, fileIngestRequest.getTableName())
                .string(JOB_ID, fileIngestRequest.getJobId())
                .number(RECEIVED_TIME, fileIngestRequest.getReceivedTime().toEpochMilli())
                .number(EXPIRY_TIME, getExpiryTimeEpochSeconds(properties, fileIngestRequest))
                .build();
    }

    public static FileIngestRequest readRecord(Map<String, AttributeValue> item) {
        return FileIngestRequest.builder()
                .pathToFile(getStringAttribute(item, FILE_PATH))
                .fileSizeBytes(getLongAttribute(item, FILE_SIZE, 0L))
                .tableName(getStringAttribute(item, TABLE_NAME))
                .jobId(getStringAttribute(item, JOB_ID))
                .receivedTime(getInstantAttribute(item, RECEIVED_TIME))
                .build();
    }

    private static long getExpiryTimeEpochSeconds(TableProperties properties, FileIngestRequest fileIngestRequest) {
        int ttlMinutes = properties.getInt(INGEST_BATCHER_TRACKING_TTL_MINUTES);
        return fileIngestRequest.getReceivedTime()
                .plus(Duration.ofMinutes(ttlMinutes))
                .getEpochSecond();
    }
}
