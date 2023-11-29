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

import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileReferenceCount;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static sleeper.core.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createBooleanAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;

class DynamoDBFileInfoFormat {
    static final String TABLE_ID = DynamoDBStateStore.TABLE_ID;
    static final String PARTITION_ID_AND_FILENAME = "PartitionIdAndFileName";
    static final String FILENAME = "FileName";
    static final String STATUS = "Status";
    static final String PARTITION_ID = "PartitionId";
    private static final String NUMBER_OF_RECORDS = "NumRecords";
    static final String LAST_UPDATE_TIME = "LastUpdateTime";
    static final String IS_COUNT_APPROXIMATE = "IsCountApproximate";
    static final String ONLY_CONTAINS_DATA_FOR_THIS_PARTITION = "OnlyContainsDataForThisPartition";
    static final String NUMBER_OF_REFERENCES = "NumReferences";
    static final String JOB_ID = "Job_name";
    private static final String DELIMITER = "|";
    private static final String DELIMITER_REGEX = Pattern.quote(DELIMITER);
    private final String sleeperTableId;

    DynamoDBFileInfoFormat(String sleeperTableId) {
        this.sleeperTableId = sleeperTableId;
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
        itemValues.put(PARTITION_ID, createStringAttribute(fileInfo.getPartitionId()));
        return createRecord(itemValues, fileInfo);
    }

    Map<String, AttributeValue> createFileReferenceRecord(FileInfo fileInfo) {
        return createRecord(createActiveFileKey(fileInfo), fileInfo);
    }

    Map<String, AttributeValue> createFileReferenceCountRecord(FileReferenceCount fileReferenceCount) {
        Map<String, AttributeValue> itemValues = createFileReferenceCountKey(fileReferenceCount);
        itemValues.put(NUMBER_OF_REFERENCES, createNumberAttribute(fileReferenceCount.getNumberOfReferences()));
        return createRecord(itemValues, fileReferenceCount);
    }

    /**
     * Creates a record for the DynamoDB state store.
     *
     * @param fileInfo the File which the record is about
     * @return A record in DynamoDB
     */
    Map<String, AttributeValue> createRecord(Map<String, AttributeValue> itemValues, FileInfo fileInfo) {
        if (null != fileInfo.getNumberOfRecords()) {
            itemValues.put(NUMBER_OF_RECORDS, createNumberAttribute(fileInfo.getNumberOfRecords()));
        }
        if (null != fileInfo.getJobId()) {
            itemValues.put(JOB_ID, createStringAttribute(fileInfo.getJobId()));
        }
        if (null != fileInfo.getLastStateStoreUpdateTime()) {
            itemValues.put(LAST_UPDATE_TIME, createNumberAttribute(fileInfo.getLastStateStoreUpdateTime()));
        }
        itemValues.put(IS_COUNT_APPROXIMATE, createBooleanAttribute(fileInfo.isCountApproximate()));
        itemValues.put(ONLY_CONTAINS_DATA_FOR_THIS_PARTITION, createBooleanAttribute(
                fileInfo.onlyContainsDataForThisPartition()));
        return itemValues;
    }

    Map<String, AttributeValue> createRecord(Map<String, AttributeValue> itemValues, FileReferenceCount fileReferenceCount) {
        if (null != fileReferenceCount.getLastUpdateTime()) {
            itemValues.put(LAST_UPDATE_TIME, createNumberAttribute(fileReferenceCount.getLastUpdateTime()));
        }
        return itemValues;
    }

    Map<String, AttributeValue> createActiveFileKey(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(PARTITION_ID_AND_FILENAME, createStringAttribute(getActiveFileSortKey(fileInfo)));
        return itemValues;
    }

    Map<String, AttributeValue> createReadyForGCKey(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(FILENAME, createStringAttribute(fileInfo.getFilename()));
        return itemValues;
    }

    Map<String, AttributeValue> createFileReferenceKey(FileInfo fileInfo) {
        return createActiveFileKey(fileInfo);
    }

    Map<String, AttributeValue> createFileReferenceCountKey(FileReferenceCount fileReferenceCount) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(FILENAME, createStringAttribute(fileReferenceCount.getFilename()));
        return itemValues;
    }

    Map<String, AttributeValue> getActiveFileKey(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(PARTITION_ID_AND_FILENAME, item.get(PARTITION_ID_AND_FILENAME));
        return itemValues;
    }

    Map<String, AttributeValue> getReadyForGCKey(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(FILENAME, item.get(FILENAME));
        return itemValues;
    }

    FileInfo getFileInfoFromAttributeValues(Map<String, AttributeValue> item) {
        FileInfo.Builder fileInfoBuilder = FileInfo.wholeFile()
                .fileStatus(FileInfo.FileStatus.valueOf(item.get(STATUS).getS()));
        if (null != item.get(PARTITION_ID_AND_FILENAME)) {
            String[] partitionIdAndFilename = splitPartitionIdAndFilename(item);
            fileInfoBuilder.partitionId(partitionIdAndFilename[0])
                    .filename(partitionIdAndFilename[1]);
        } else {
            fileInfoBuilder.partitionId(item.get(PARTITION_ID).getS())
                    .filename(item.get(FILENAME).getS());
        }
        if (null != item.get(NUMBER_OF_RECORDS)) {
            fileInfoBuilder.numberOfRecords(Long.parseLong(item.get(NUMBER_OF_RECORDS).getN()));
        }
        if (null != item.get(JOB_ID)) {
            fileInfoBuilder.jobId(item.get(JOB_ID).getS());
        }
        if (null != item.get(LAST_UPDATE_TIME)) {
            fileInfoBuilder.lastStateStoreUpdateTime(Long.parseLong(item.get(LAST_UPDATE_TIME).getN()));
        }
        fileInfoBuilder.countApproximate(item.get(IS_COUNT_APPROXIMATE).getBOOL());
        fileInfoBuilder.onlyContainsDataForThisPartition(item.get(ONLY_CONTAINS_DATA_FOR_THIS_PARTITION).getBOOL());
        return fileInfoBuilder.build();
    }

    FileReferenceCount getFileReferenceCountFromAttributeValues(Map<String, AttributeValue> item) {
        FileReferenceCount.Builder builder = FileReferenceCount.builder()
                .filename(item.get(FILENAME).getS())
                .numberOfReferences(Long.parseLong(item.get(NUMBER_OF_REFERENCES).getN()))
                .tableId(item.get(TABLE_ID).getS());
        if (null != item.get(LAST_UPDATE_TIME)) {
            builder.lastUpdateTime(Long.parseLong(item.get(LAST_UPDATE_TIME).getN()));
        }
        return builder.build();
    }

    static String getActiveFileSortKey(FileInfo fileInfo) {
        return fileInfo.getPartitionId() + DELIMITER + fileInfo.getFilename();
    }

    private static String[] splitPartitionIdAndFilename(Map<String, AttributeValue> item) {
        return item.get(PARTITION_ID_AND_FILENAME).getS().split(DELIMITER_REGEX);
    }
}
