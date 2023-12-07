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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.IdempotentParameterMismatchException;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.TransactionInProgressException;
import com.amazonaws.services.dynamodbv2.model.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.statestore.AssignJobToFilesRequest;
import sleeper.core.statestore.StateStoreException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.LAST_UPDATE_TIME;

public class DynamoDBAssignJobsToFiles {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBAssignJobsToFiles.class);

    private final String activeTableName;
    private final AmazonDynamoDB dynamoDB;
    private Clock clock = Clock.systemUTC();

    public DynamoDBAssignJobsToFiles(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.activeTableName = instanceProperties.get(ACTIVE_FILEINFO_TABLENAME);
        this.dynamoDB = dynamoDB;
    }

    public void updateDynamoDB(List<AssignJobToFilesRequest> jobs) throws StateStoreException {

        // Create updates for each of the files, conditional on the compactionJob field being not present
        long updateTime = clock.millis();
        List<TransactWriteItem> batch = new ArrayList<>();
        for (AssignJobToFilesRequest job : jobs) {
            List<TransactWriteItem> writes = job.getFiles().stream().map(filename ->
                            new TransactWriteItem().withUpdate(new Update()
                                    .withTableName(activeTableName)
                                    .withKey(new DynamoDBFileInfoFormat(job.getTableId()).createActiveFileKeyWithPartitionAndFilename(
                                            job.getPartitionId(), filename))
                                    .withUpdateExpression("SET #jobid = :jobid, #time = :time")
                                    .withConditionExpression("attribute_exists(#time) and attribute_not_exists(#jobid)")
                                    .withExpressionAttributeNames(Map.of(
                                            "#jobid", JOB_ID,
                                            "#time", LAST_UPDATE_TIME))
                                    .withExpressionAttributeValues(Map.of(
                                            ":jobid", createStringAttribute(job.getJobId()),
                                            ":time", createNumberAttribute(updateTime)))))
                    .collect(Collectors.toUnmodifiableList());
            if (batch.size() + writes.size() > 100) {
                transactWriteItems(batch);
                batch.clear();
            }
            batch.addAll(writes);
        }
        if (!batch.isEmpty()) {
            transactWriteItems(batch);
        }
    }

    private void transactWriteItems(List<TransactWriteItem> writes) throws StateStoreException {
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated job status with {} writes, read capacity consumed = {}",
                    writes.size(), totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }
}
