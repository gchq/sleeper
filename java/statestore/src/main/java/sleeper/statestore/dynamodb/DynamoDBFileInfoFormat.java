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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static sleeper.core.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;

class DynamoDBFileInfoFormat {
    static final String TABLE_NAME = DynamoDBStateStore.TABLE_NAME;
    static final String PARTITION_AND_FILENAME = "PartitionAndFileName";
    static final String NAME = "Name";
    static final String STATUS = "Status";
    static final String PARTITION = "Partition";
    private static final String NUMBER_LINES = "NumLines";
    static final String LAST_UPDATE_TIME = "LastUpdateTime";
    static final String JOB_ID = "Job_name";
    private static final String DELIMETER = "|";
    private final String sleeperTableName;
    private final List<PrimitiveType> rowKeyTypes;

    DynamoDBFileInfoFormat(String sleeperTableName, Schema schema) {
        this.sleeperTableName = sleeperTableName;
        this.rowKeyTypes = schema.getRowKeyTypes();
    }

    Map<String, AttributeValue> createRecordWithJobId(FileInfo fileInfo, String jobId) {
        Map<String, AttributeValue> record = createRecord(fileInfo);
        record.put(JOB_ID, createStringAttribute(jobId));
        return record;
    }

    Map<String, AttributeValue> createRecord(FileInfo fileInfo) {
        if (ACTIVE == fileInfo.getFileStatus()) {
            return createActiveFileRecord(fileInfo);
        } else {
            return createReadyForGCRecord(fileInfo);
        }
    }

    Map<String, AttributeValue> createActiveFileRecord(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = createActiveFileKey(fileInfo);
        itemValues.put(STATUS, createStringAttribute(ACTIVE.toString()));
        return createRecord(itemValues, fileInfo);
    }

    Map<String, AttributeValue> createReadyForGCRecord(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = createReadyForGCKey(fileInfo);
        itemValues.put(STATUS, createStringAttribute(READY_FOR_GARBAGE_COLLECTION.toString()));
        return createRecord(itemValues, fileInfo);
    }

    /**
     * Creates a record for the DynamoDB state store.
     *
     * @param fileInfo the File which the record is about
     * @return A record in DynamoDB
     */
    Map<String, AttributeValue> createRecord(Map<String, AttributeValue> itemValues, FileInfo fileInfo) {
        itemValues.put(PARTITION, createStringAttribute(fileInfo.getPartitionId()));
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

    Map<String, AttributeValue> createActiveFileKey(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_NAME, createStringAttribute(sleeperTableName));
        itemValues.put(PARTITION_AND_FILENAME, createStringAttribute(createActiveFileSortKey(fileInfo)));
        return itemValues;
    }

    Map<String, AttributeValue> createReadyForGCKey(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_NAME, createStringAttribute(sleeperTableName));
        itemValues.put(NAME, createStringAttribute(fileInfo.getFilename()));
        return itemValues;
    }

    Map<String, AttributeValue> getActiveFileKey(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_NAME, createStringAttribute(sleeperTableName));
        itemValues.put(PARTITION_AND_FILENAME, item.get(PARTITION_AND_FILENAME));
        return itemValues;
    }

    Map<String, AttributeValue> getReadyForGCKey(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_NAME, createStringAttribute(sleeperTableName));
        itemValues.put(NAME, item.get(NAME));
        return itemValues;
    }

    FileInfo getFileInfoFromAttributeValues(Map<String, AttributeValue> item) {
        FileInfo.Builder fileInfoBuilder = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .fileStatus(FileInfo.FileStatus.valueOf(item.get(STATUS).getS()))
                .partitionId(item.get(PARTITION).getS());
        if (null != item.get(NAME)) {
            fileInfoBuilder.filename(item.get(NAME).getS());
        } else {
            fileInfoBuilder.filename(getFilenameFromSortKey(item));
        }
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

    private static String createActiveFileSortKey(FileInfo fileInfo) {
        return fileInfo.getPartitionId() + DELIMETER + fileInfo.getFilename();
    }

    private static String getFilenameFromSortKey(Map<String, AttributeValue> item) {
        return item.get(PARTITION_AND_FILENAME).getS().split(Pattern.quote(DELIMETER))[1];
    }
}
