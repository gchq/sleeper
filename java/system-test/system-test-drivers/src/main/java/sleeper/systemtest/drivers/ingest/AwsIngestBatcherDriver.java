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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.submitter.FileIngestRequestSerDe;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_FUNCTION;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class AwsIngestBatcherDriver implements IngestBatcherDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsIngestBatcherDriver.class);

    private final SystemTestInstanceContext instance;
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonSQS sqsClient;
    private final LambdaClient lambdaClient;
    private final PollWithRetries pollBatcherStore = PollWithRetries
            .intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(2));

    public AwsIngestBatcherDriver(
            SystemTestInstanceContext instance,
            IngestSourceFilesContext sourceFiles,
            SystemTestClients clients) {
        this.instance = instance;
        this.dynamoDBClient = clients.getDynamoDB();
        this.sqsClient = clients.getSqs();
        this.lambdaClient = clients.getLambda();
    }

    public void sendFiles(List<String> files) {
        LOGGER.info("Sending {} files to ingest batcher queue", files.size());
        int filesBefore = batcherStore().getPendingFilesOldestFirst().size();
        int filesAfter = filesBefore + files.size();
        sqsClient.sendMessage(instance.getInstanceProperties().get(INGEST_BATCHER_SUBMIT_QUEUE_URL),
                FileIngestRequestSerDe.toJson(files,
                        instance.getTableProperties().get(TABLE_NAME)));
        try {
            pollBatcherStore.pollUntil("files appear in batcher store", () -> {
                List<FileIngestRequest> pending = batcherStore().getPendingFilesOldestFirst();
                LOGGER.info("Found pending files in batcher store: {}", pending);
                return pending.size() == filesAfter;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public List<String> invokeGetJobIds() {
        LOGGER.info("Triggering ingest batcher job creation lambda");
        Set<String> jobIdsBefore = getAllJobIdsInStore().collect(Collectors.toSet());
        InvokeLambda.invokeWith(lambdaClient,
                instance.getInstanceProperties().get(INGEST_BATCHER_JOB_CREATION_FUNCTION));
        return getAllJobIdsInStore()
                .filter(not(jobIdsBefore::contains))
                .collect(toUnmodifiableList());
    }

    private Stream<String> getAllJobIdsInStore() {
        return batcherStore().getAllFilesNewestFirst().stream()
                .filter(FileIngestRequest::isAssignedToJob)
                .map(FileIngestRequest::getJobId)
                .distinct();
    }

    public void clearStore() {
        batcherStore().deleteAllPending();
    }

    public IngestBatcherStore batcherStore() {
        return new DynamoDBIngestBatcherStore(dynamoDBClient,
                instance.getInstanceProperties(), instance.getTablePropertiesProvider());
    }
}
