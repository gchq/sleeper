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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.StateStoreFile;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createBooleanAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;

/**
 * Reads and writes DynamoDB items to store Sleeper file references and reference counts.
 */
class DynamoDBFileReferenceFormat {
    static final String TABLE_ID = DynamoDBStateStore.TABLE_ID;
    static final String PARTITION_ID_AND_FILENAME = "PartitionIdAndFileName";
    static final String FILENAME = "FileName";
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

    DynamoDBFileReferenceFormat(String sleeperTableId) {
        this.sleeperTableId = sleeperTableId;
    }

    Map<String, AttributeValue> createRecord(FileReference fileReference) {
        return createActiveFileRecord(fileReference);
    }

    Map<String, AttributeValue> createActiveFileRecord(FileReference fileReference) {
        Map<String, AttributeValue> itemValues = createActiveFileKey(fileReference);
        return createRecord(itemValues, fileReference);
    }

    /**
     * Creates a record for the DynamoDB state store.
     *
     * @param  fileReference the File which the record is about
     * @return               A record in DynamoDB
     */
    Map<String, AttributeValue> createRecord(Map<String, AttributeValue> itemValues, FileReference fileReference) {
        if (null != fileReference.getNumberOfRecords()) {
            itemValues.put(NUMBER_OF_RECORDS, createNumberAttribute(fileReference.getNumberOfRecords()));
        }
        if (null != fileReference.getJobId()) {
            itemValues.put(JOB_ID, createStringAttribute(fileReference.getJobId()));
        }
        if (null != fileReference.getLastStateStoreUpdateTime()) {
            itemValues.put(LAST_UPDATE_TIME, createInstantAttribute(fileReference.getLastStateStoreUpdateTime()));
        }
        itemValues.put(IS_COUNT_APPROXIMATE, createBooleanAttribute(fileReference.isCountApproximate()));
        itemValues.put(ONLY_CONTAINS_DATA_FOR_THIS_PARTITION, createBooleanAttribute(
                fileReference.onlyContainsDataForThisPartition()));
        return itemValues;
    }

    Map<String, AttributeValue> createActiveFileKey(FileReference fileReference) {
        return createActiveFileKey(fileReference.getPartitionId(), fileReference.getFilename());
    }

    Map<String, AttributeValue> createActiveFileKey(String partitionId, String filename) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(PARTITION_ID_AND_FILENAME, createStringAttribute(partitionId + DELIMITER + filename));
        return itemValues;
    }

    Map<String, AttributeValue> createReferenceCountKey(String filename) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(FILENAME, createStringAttribute(filename));
        return itemValues;
    }

    Map<String, AttributeValue> createReferenceCountRecord(String filename, Instant lastUpdateTime, int referenceCount) {
        Map<String, AttributeValue> itemValues = createReferenceCountKey(filename);
        itemValues.put(REFERENCES, createNumberAttribute(referenceCount));
        itemValues.put(LAST_UPDATE_TIME, createInstantAttribute(lastUpdateTime));
        return itemValues;
    }

    Map<String, AttributeValue> getActiveFileKey(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_ID, createStringAttribute(sleeperTableId));
        itemValues.put(PARTITION_ID_AND_FILENAME, item.get(PARTITION_ID_AND_FILENAME));
        return itemValues;
    }

    FileReference getFileReferenceFromAttributeValues(Map<String, AttributeValue> item) {
        FileReference.Builder fileReferenceBuilder = FileReference.builder();
        if (null != item.get(PARTITION_ID_AND_FILENAME)) {
            String[] partitionIdAndFilename = splitPartitionIdAndFilename(item);
            fileReferenceBuilder.partitionId(partitionIdAndFilename[0])
                    .filename(partitionIdAndFilename[1]);
        } else {
            fileReferenceBuilder.partitionId(item.get(PARTITION_ID).getS())
                    .filename(item.get(FILENAME).getS());
        }
        if (null != item.get(NUMBER_OF_RECORDS)) {
            fileReferenceBuilder.numberOfRecords(Long.parseLong(item.get(NUMBER_OF_RECORDS).getN()));
        }
        if (null != item.get(JOB_ID)) {
            fileReferenceBuilder.jobId(item.get(JOB_ID).getS());
        }
        if (null != item.get(LAST_UPDATE_TIME)) {
            fileReferenceBuilder.lastStateStoreUpdateTime(getInstantAttribute(item, LAST_UPDATE_TIME));
        }
        fileReferenceBuilder.countApproximate(item.get(IS_COUNT_APPROXIMATE).getBOOL());
        fileReferenceBuilder.onlyContainsDataForThisPartition(item.get(ONLY_CONTAINS_DATA_FOR_THIS_PARTITION).getBOOL());
        return fileReferenceBuilder.build();
    }

    private static String[] splitPartitionIdAndFilename(Map<String, AttributeValue> item) {
        return item.get(PARTITION_ID_AND_FILENAME).getS().split(DELIMITER_REGEX);
    }

    public String getFilenameFromReferenceCount(Map<String, AttributeValue> item) {
        return item.get(FILENAME).getS();
    }

    public AllReferencesToAFile getReferencedFile(
            Map<String, AttributeValue> referenceCountItem,
            Map<String, List<FileReference>> referencesByFilename) {
        String filename = getFilenameFromReferenceCount(referenceCountItem);
        Instant lastUpdateTime = getInstantAttribute(referenceCountItem, LAST_UPDATE_TIME);
        List<FileReference> references = referencesByFilename.getOrDefault(filename, List.of());
        return new StateStoreFile(filename, lastUpdateTime, references).toModel();
    }
}
