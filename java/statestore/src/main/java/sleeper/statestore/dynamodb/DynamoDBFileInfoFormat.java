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

import sleeper.core.key.Key;
import sleeper.core.key.KeySerDe;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStoreException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createBinaryAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;

class DynamoDBFileInfoFormat {

    static final String NAME = "Name";
    static final String STATUS = "Status";
    static final String PARTITION = "Partition";
    private static final String NUMBER_LINES = "NumLines";
    private static final String MIN_KEY = "MinKey";
    private static final String MAX_KEY = "MaxKey";
    static final String LAST_UPDATE_TIME = "LastUpdateTime";
    static final String JOB_ID = "Job_name";

    private final List<PrimitiveType> rowKeyTypes;
    private final KeySerDe keySerDe;

    DynamoDBFileInfoFormat(Schema schema) {
        rowKeyTypes = schema.getRowKeyTypes();
        keySerDe = new KeySerDe(rowKeyTypes);
    }

    /**
     * Creates a record with a new status
     *
     * @param fileInfo  the File
     * @param newStatus the new status of that file
     * @return A Dynamo record
     * @throws StateStoreException if the Dynamo record fails to be created
     */
    Map<String, AttributeValue> createRecordWithStatus(FileInfo fileInfo, FileInfo.FileStatus newStatus) throws StateStoreException {
        Map<String, AttributeValue> record = createRecord(fileInfo);
        record.put(STATUS, createStringAttribute(newStatus.toString()));
        return record;
    }

    Map<String, AttributeValue> createRecordWithJobId(FileInfo fileInfo, String jobId) throws StateStoreException {
        Map<String, AttributeValue> record = createRecord(fileInfo);
        record.put(JOB_ID, createStringAttribute(jobId));
        return record;
    }

    /**
     * Creates a record for the DynamoDB state store.
     *
     * @param fileInfo the File which the record is about
     * @return A record in DynamoDB
     * @throws StateStoreException if the record fails to create
     */
    Map<String, AttributeValue> createRecord(FileInfo fileInfo) throws StateStoreException {
        Map<String, AttributeValue> itemValues = new HashMap<>();

        itemValues.put(NAME, createStringAttribute(fileInfo.getFilename()));
        itemValues.put(PARTITION, createStringAttribute(fileInfo.getPartitionId()));
        itemValues.put(STATUS, createStringAttribute(fileInfo.getFileStatus().toString()));
        if (null != fileInfo.getNumberOfRecords()) {
            itemValues.put(NUMBER_LINES, createNumberAttribute(fileInfo.getNumberOfRecords()));
        }
        try {
            if (null != fileInfo.getMinRowKey()) {
                itemValues.put(MIN_KEY, getAttributeValueFromRowKeys(fileInfo.getMinRowKey()));
            }
            if (null != fileInfo.getMaxRowKey()) {
                itemValues.put(MAX_KEY, getAttributeValueFromRowKeys(fileInfo.getMaxRowKey()));
            }
        } catch (IOException e) {
            throw new StateStoreException("IOException serialising row keys", e);
        }
        if (null != fileInfo.getJobId()) {
            itemValues.put(JOB_ID, createStringAttribute(fileInfo.getJobId()));
        }
        if (null != fileInfo.getLastStateStoreUpdateTime()) {
            itemValues.put(LAST_UPDATE_TIME, createNumberAttribute(fileInfo.getLastStateStoreUpdateTime()));
        }

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
        if (null != item.get(MIN_KEY)) {
            fileInfoBuilder.minRowKey(keySerDe.deserialise(item.get(MIN_KEY).getB().array()));
        }
        if (null != item.get(MAX_KEY)) {
            fileInfoBuilder.maxRowKey(keySerDe.deserialise(item.get(MAX_KEY).getB().array()));
        }
        if (null != item.get(JOB_ID)) {
            fileInfoBuilder.jobId(item.get(JOB_ID).getS());
        }
        if (null != item.get(LAST_UPDATE_TIME)) {
            fileInfoBuilder.lastStateStoreUpdateTime(Long.parseLong(item.get(LAST_UPDATE_TIME).getN()));
        }
        return fileInfoBuilder.build();
    }

    private AttributeValue getAttributeValueFromRowKeys(Key rowKey) throws IOException {
        return createBinaryAttribute(keySerDe.serialise(rowKey));
    }
}
