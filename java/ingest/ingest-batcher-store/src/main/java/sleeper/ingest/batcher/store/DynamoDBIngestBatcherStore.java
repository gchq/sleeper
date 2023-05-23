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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBIngestBatcherStore implements IngestBatcherStore {

    private final AmazonDynamoDB dynamoDB;
    private final String requestsTableName;
    private final TablePropertiesProvider tablePropertiesProvider;

    public DynamoDBIngestBatcherStore(AmazonDynamoDB dynamoDB,
                                      InstanceProperties instanceProperties,
                                      TablePropertiesProvider tablePropertiesProvider) {
        this.dynamoDB = dynamoDB;
        this.requestsTableName = ingestRequestsTableName(instanceProperties.get(ID));
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    public static String ingestRequestsTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-batcher-store");
    }

    @Override
    public void addFile(FileIngestRequest fileIngestRequest) {
        // TODO handle existing record for file
        dynamoDB.putItem(new PutItemRequest()
                .withTableName(requestsTableName)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withItem(DynamoDBIngestRequestFormat.createRecord(tablePropertiesProvider, fileIngestRequest)));
    }

    @Override
    public void assignJob(String jobId, List<FileIngestRequest> filesInJob) {
        // TODO perform atomically, fail if job already assigned
        filesInJob.forEach(file -> addFile(file.toBuilder().jobId(jobId).build()));
    }

    @Override
    public List<FileIngestRequest> getAllFilesNewestFirst() {
        return streamPagedItems(dynamoDB, new ScanRequest()
                .withTableName(requestsTableName))
                .map(DynamoDBIngestRequestFormat::readRecord)
                .sorted(comparing(FileIngestRequest::getReceivedTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public List<FileIngestRequest> getPendingFilesOldestFirst() {
        // TODO query for requests not assigned to a job
        return streamPagedItems(dynamoDB, new ScanRequest()
                .withTableName(requestsTableName))
                .map(DynamoDBIngestRequestFormat::readRecord)
                .filter(not(FileIngestRequest::isAssignedToJob))
                .sorted(comparing(FileIngestRequest::getReceivedTime))
                .collect(Collectors.toList());
    }
}
