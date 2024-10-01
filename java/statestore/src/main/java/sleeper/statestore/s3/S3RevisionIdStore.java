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
package sleeper.statestore.s3;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;

import java.util.Map;

import static sleeper.statestore.s3.S3StateStore.CURRENT_FILES_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.CURRENT_PARTITIONS_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.CURRENT_REVISION;
import static sleeper.statestore.s3.S3StateStore.CURRENT_UUID;
import static sleeper.statestore.s3.S3StateStore.REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.TABLE_ID;

/**
 * Handles storing revision IDs that track the latest version of a file in S3. Whenever we update one of the underlying
 * files used to store the state store data, this results in a new revision ID, as well as a new file in S3 which
 * contains the new data. The revision ID is stored in DynamoDB, and it acts as a pointer to the file in S3.
 */
public class S3RevisionIdStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3RevisionIdStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String dynamoRevisionIdTable;
    private final String sleeperTableId;
    private final boolean stronglyConsistentReads;

    public S3RevisionIdStore(AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties, TableProperties tableProperties) {
        this.dynamoDB = dynamoDB;
        this.dynamoRevisionIdTable = instanceProperties.get(CdkDefinedInstanceProperty.REVISION_TABLENAME);
        this.sleeperTableId = tableProperties.get(TableProperty.TABLE_ID);
        this.stronglyConsistentReads = tableProperties.getBoolean(TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS);
    }

    public S3RevisionId getCurrentPartitionsRevisionId() {
        return getCurrentRevisionId(CURRENT_PARTITIONS_REVISION_ID_KEY);
    }

    public S3RevisionId getCurrentFilesRevisionId() {
        return getCurrentRevisionId(CURRENT_FILES_REVISION_ID_KEY);
    }

    S3RevisionId getCurrentRevisionId(String revisionIdKey) {
        GetItemResult result = dynamoDB.getItem(new GetItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withConsistentRead(stronglyConsistentReads)
                .withKey(Map.of(
                        TABLE_ID, new AttributeValue(sleeperTableId),
                        REVISION_ID_KEY, new AttributeValue(revisionIdKey))));
        if (null == result || null == result.getItem() || result.getItem().isEmpty()) {
            return null;
        }
        Map<String, AttributeValue> map = result.getItem();
        String revision = map.get(CURRENT_REVISION).getS();
        String uuid = map.get(CURRENT_UUID).getS();
        return new S3RevisionId(revision, uuid);
    }

    void saveFirstPartitionRevision(S3RevisionId revisionId) {
        saveFirstRevision(CURRENT_PARTITIONS_REVISION_ID_KEY, revisionId);
    }

    void saveFirstFilesRevision(S3RevisionId revisionId) {
        saveFirstRevision(CURRENT_FILES_REVISION_ID_KEY, revisionId);
    }

    private void saveFirstRevision(String revisionIdKey, S3RevisionId revisionId) {
        Map<String, AttributeValue> item = createRevisionIdItem(revisionIdKey, revisionId);
        dynamoDB.putItem(new PutItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withItem(item));
        LOGGER.debug("Put item to DynamoDB (item = {}, table = {})", item, dynamoRevisionIdTable);
    }

    void deletePartitionsRevision() {
        deleteRevision(CURRENT_PARTITIONS_REVISION_ID_KEY);
    }

    void deleteFilesRevision() {
        deleteRevision(CURRENT_FILES_REVISION_ID_KEY);
    }

    private void deleteRevision(String revisionIdValue) {
        dynamoDB.deleteItem(new DeleteItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withKey(Map.of(
                        TABLE_ID, new AttributeValue().withS(sleeperTableId),
                        REVISION_ID_KEY, new AttributeValue().withS(revisionIdValue))));
    }

    void conditionalUpdateOfRevisionId(String revisionIdKey, S3RevisionId currentRevisionId, S3RevisionId newRevisionId) {
        LOGGER.debug("Attempting conditional update of {} from revision id {} to {}", revisionIdKey, currentRevisionId, newRevisionId);
        dynamoDB.putItem(new PutItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withItem(createRevisionIdItem(revisionIdKey, newRevisionId))
                .withConditionExpression("#CurrentRevision = :currentrevision and #CurrentUUID = :currentuuid")
                .withExpressionAttributeNames(Map.of(
                        "#CurrentRevision", CURRENT_REVISION,
                        "#CurrentUUID", CURRENT_UUID))
                .withExpressionAttributeValues(Map.of(
                        ":currentrevision", new AttributeValue(currentRevisionId.getRevision()),
                        ":currentuuid", new AttributeValue(currentRevisionId.getUuid()))));
    }

    private Map<String, AttributeValue> createRevisionIdItem(String revisionIdKey, S3RevisionId revisionId) {
        return Map.of(
                TABLE_ID, new AttributeValue().withS(sleeperTableId),
                REVISION_ID_KEY, new AttributeValue().withS(revisionIdKey),
                CURRENT_REVISION, new AttributeValue().withS(revisionId.getRevision()),
                CURRENT_UUID, new AttributeValue().withS(revisionId.getUuid()));
    }

}
