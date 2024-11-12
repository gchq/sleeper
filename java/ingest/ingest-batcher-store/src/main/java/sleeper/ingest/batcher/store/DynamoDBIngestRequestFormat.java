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

package sleeper.ingest.batcher.store;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.batcher.core.FileIngestRequest;

import java.time.Duration;
import java.util.Map;

import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_TRACKING_TTL_MINUTES;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;

public class DynamoDBIngestRequestFormat {
    private DynamoDBIngestRequestFormat() {
    }

    public static final String FILE_PATH = "FilePath";
    public static final String TABLE_NAME = "TableName";
    public static final String FILE_SIZE = "FileSize";
    public static final String JOB_ID = "JobId";
    public static final String RECEIVED_TIME = "ReceivedTime";
    public static final String EXPIRY_TIME = "ExpiryTime";

    public static final String NOT_ASSIGNED_TO_JOB = "not_assigned_to_job";

    public static Map<String, AttributeValue> createRecord(
            TablePropertiesProvider tablePropertiesProvider, FileIngestRequest fileIngestRequest) {
        TableProperties properties = tablePropertiesProvider.getById(fileIngestRequest.getTableId());
        return new DynamoDBRecordBuilder()
                .string(FILE_PATH, fileIngestRequest.getTableId() + "/" + fileIngestRequest.getFile())
                .number(FILE_SIZE, fileIngestRequest.getFileSizeBytes())
                .string(JOB_ID, getJobIdOrUnassigned(fileIngestRequest))
                .number(RECEIVED_TIME, fileIngestRequest.getReceivedTime().toEpochMilli())
                .number(EXPIRY_TIME, getExpiryTimeEpochSeconds(properties, fileIngestRequest))
                .build();
    }

    public static FileIngestRequest readRecord(Map<String, AttributeValue> item) {
        String fullPath = getStringAttribute(item, FILE_PATH);
        int pathSeparatorIndex = fullPath.indexOf('/');
        String tableId = fullPath.substring(0, pathSeparatorIndex);
        String filePath = fullPath.substring(pathSeparatorIndex + 1);
        return FileIngestRequest.builder()
                .file(filePath)
                .fileSizeBytes(getLongAttribute(item, FILE_SIZE, 0L))
                .tableId(tableId)
                .jobId(getJobIdAttribute(item))
                .receivedTime(getInstantAttribute(item, RECEIVED_TIME))
                .build();
    }

    public static Map<String, AttributeValue> createUnassignedKey(FileIngestRequest fileIngestRequest) {
        return new DynamoDBRecordBuilder()
                .string(JOB_ID, NOT_ASSIGNED_TO_JOB)
                .string(FILE_PATH, fileIngestRequest.getTableId() + "/" + fileIngestRequest.getFile())
                .build();
    }

    private static long getExpiryTimeEpochSeconds(TableProperties properties, FileIngestRequest fileIngestRequest) {
        int ttlMinutes = properties.getInt(INGEST_BATCHER_TRACKING_TTL_MINUTES);
        return fileIngestRequest.getReceivedTime()
                .plus(Duration.ofMinutes(ttlMinutes))
                .getEpochSecond();
    }

    private static String getJobIdOrUnassigned(FileIngestRequest fileIngestRequest) {
        if (fileIngestRequest.isAssignedToJob()) {
            return fileIngestRequest.getJobId();
        } else {
            return NOT_ASSIGNED_TO_JOB;
        }
    }

    private static String getJobIdAttribute(Map<String, AttributeValue> item) {
        String value = getStringAttribute(item, JOB_ID);
        if (NOT_ASSIGNED_TO_JOB.equals(value)) {
            return null;
        } else {
            return value;
        }
    }
}
