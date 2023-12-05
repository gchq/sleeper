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
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;

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
    static final String JOB_ID = "Job_name";
    static final String REFERENCES = "References";
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

    Map<String, AttributeValue> createActiveFileKey(FileInfo fileInfo) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(PARTITION_ID_AND_FILENAME, createStringAttribute(getActiveFileSortKey(fileInfo)));
        return itemValues;
    }

    Map<String, AttributeValue> createActiveFileKeyWithPartitionAndFilename(String partitionId, String filename) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(PARTITION_ID_AND_FILENAME, createStringAttribute(
                getActiveFileSortKeyWithPartitionAndFilename(partitionId, filename)));
        return itemValues;
    }

    Map<String, AttributeValue> createReadyForGCKey(FileInfo fileInfo) {
        return createReadyForGCKey(fileInfo.getFilename());
    }

    Map<String, AttributeValue> createReadyForGCKey(String filename) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(FILENAME, createStringAttribute(filename));
        return itemValues;
    }

    Map<String, AttributeValue> createReferenceCountKey(FileInfo fileInfo) {
        return createReferenceCountKey(fileInfo.getFilename());
    }

    Map<String, AttributeValue> createReferenceCountKey(String filename) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(FILENAME, createStringAttribute(filename));
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

    static String getActiveFileSortKey(FileInfo fileInfo) {
        return fileInfo.getPartitionId() + DELIMITER + fileInfo.getFilename();
    }

    static String getActiveFileSortKeyWithPartitionAndFilename(String partitionId, String filename) {
        return partitionId + DELIMITER + filename;
    }

    private static String[] splitPartitionIdAndFilename(Map<String, AttributeValue> item) {
        return item.get(PARTITION_ID_AND_FILENAME).getS().split(DELIMITER_REGEX);
    }

    public String getFilenameFromReferenceCount(Map<String, AttributeValue> item) {
        return item.get(FILENAME).getS();
    }

    public FileReferenceCount getFileReferenceCountFromAttributeValues(Map<String, AttributeValue> item) {
        return FileReferenceCount.builder()
                .filename(item.get(FILENAME).getS())
                .references(getIntAttribute(item, REFERENCES, 0))
                .lastUpdateTime(getInstantAttribute(item, LAST_UPDATE_TIME))
                .build();
    }
}
