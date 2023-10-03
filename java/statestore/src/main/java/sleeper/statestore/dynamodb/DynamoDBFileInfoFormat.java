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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.statestore.FileInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;

class DynamoDBFileInfoFormat {

    static final String TABLE_NAME = DynamoDBStateStore.TABLE_NAME;
    static final String NAME = "Name";
    static final String STATUS = "Status";
    static final String PARTITION = "Partition";
    private static final String NUMBER_LINES = "NumLines";
    static final String LAST_UPDATE_TIME = "LastUpdateTime";
    static final String JOB_ID = "Job_name";

    private final String sleeperTableName;
    private final List<PrimitiveType> rowKeyTypes;

    DynamoDBFileInfoFormat(String sleeperTableName, Schema schema) {
        this.sleeperTableName = sleeperTableName;
        this.rowKeyTypes = schema.getRowKeyTypes();
    }

    /**
     * Creates a record with a new status
     *
     * @param fileInfo  the File
     * @param newStatus the new status of that file
     * @return A Dynamo record
     */
    Map<String, AttributeValue> createRecordWithStatus(FileInfo fileInfo, FileInfo.FileStatus newStatus) {
        Map<String, AttributeValue> record = createRecord(fileInfo);
        record.put(STATUS, createStringAttribute(newStatus.toString()));
        return record;
    }

    Map<String, AttributeValue> createRecordWithJobId(FileInfo fileInfo, String jobId) {
        Map<String, AttributeValue> record = createRecord(fileInfo);
        record.put(JOB_ID, createStringAttribute(jobId));
        return record;
    }

    /**
     * Creates a record for the DynamoDB state store.
     *
     * @param fileInfo the File which the record is about
     * @return A record in DynamoDB
     */
    Map<String, AttributeValue> createRecord(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = new HashMap<>();

        itemValues.put(TABLE_NAME, createStringAttribute(sleeperTableName));
        itemValues.put(NAME, createStringAttribute(fileInfo.getFilename()));
        itemValues.put(PARTITION, createStringAttribute(fileInfo.getPartitionId()));
        itemValues.put(STATUS, createStringAttribute(fileInfo.getFileStatus().toString()));
        if (null != fileInfo.getNumberOfRecords()) {
            itemValues.put(NUMBER_LINES, createNumberAttribute(fileInfo.getNumberOfRecords()));
        }
        if (null != fileInfo.getJobId()) {
            itemValues.put(JOB_ID, createStringAttribute(fileInfo.getJobId()));
        }
        if (null != fileInfo.getLastStateStoreUpdateTime()) {
            itemValues.put(LAST_UPDATE_TIME, createNumberAttribute(fileInfo.getLastStateStoreUpdateTime()));
        }
        return itemValues;
    }

    Map<String, AttributeValue> createKey(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_NAME, createStringAttribute(sleeperTableName));
        itemValues.put(NAME, createStringAttribute(fileInfo.getFilename()));
        return itemValues;
    }

    Map<String, AttributeValue> getKey(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_NAME, createStringAttribute(sleeperTableName));
        itemValues.put(NAME, item.get(NAME));
        return itemValues;
    }

    FileInfo getFileInfoFromAttributeValues(Map<String, AttributeValue> item) throws IOException {
        FileInfo.Builder fileInfoBuilder = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .fileStatus(FileInfo.FileStatus.valueOf(item.get(STATUS).getS()))
                .partitionId(item.get(PARTITION).getS())
                .filename(item.get(NAME).getS());
        if (null != item.get(NUMBER_LINES)) {
            fileInfoBuilder.numberOfRecords(Long.parseLong(item.get(NUMBER_LINES).getN()));
        }
        if (null != item.get(JOB_ID)) {
            fileInfoBuilder.jobId(item.get(JOB_ID).getS());
        }
        if (null != item.get(LAST_UPDATE_TIME)) {
            fileInfoBuilder.lastStateStoreUpdateTime(Long.parseLong(item.get(LAST_UPDATE_TIME).getN()));
        }
        return fileInfoBuilder.build();
    }
}
