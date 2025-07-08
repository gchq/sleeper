/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.systemtest.drivers.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Schema;
import sleeper.core.util.PollWithRetries;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.systemtest.drivers.util.ReadRecordsFromS3;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesSendAndWaitDriver;
import sleeper.systemtest.dsl.query.QuerySendAndWaitDriver;

import java.time.Duration;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class SQSQueryDriver implements QuerySendAndWaitDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQSQueryDriver.class);

    private final SystemTestInstanceContext instance;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final SqsClient sqsClient;
    private final SystemTestClients clients;
    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(2), Duration.ofMinutes(1));

    public SQSQueryDriver(
            SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.s3Client = clients.getS3();
        this.dynamoClient = clients.getDynamo();
        this.sqsClient = clients.getSqs();
        this.clients = clients;
    }

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        return new QueryAllTablesSendAndWaitDriver(instance,
                new SQSQueryDriver(instance, clients));
    }

    @Override
    public void send(Query query) {
        sqsClient.sendMessage(request -> request
                .queueUrl(instance.getInstanceProperties().get(QUERY_QUEUE_URL))
                .messageBody(new QuerySerDe(new SchemaLoaderFromInstanceContext(instance)).toJson(query)));
    }

    @Override
    public void waitFor(Query query) {
        QueryTrackerStore queryTracker = new DynamoDBQueryTracker(instance.getInstanceProperties(), dynamoClient);
        try {
            poll.pollUntil("query is finished: " + query.getQueryId(), () -> {
                try {
                    TrackedQuery queryStatus = queryTracker.getStatus(query.getQueryId());
                    if (queryStatus == null) {
                        LOGGER.info("Query not yet in tracker: {}", query.getQueryId());
                        return false;
                    }
                    QueryState state = queryStatus.getLastKnownState();
                    if (QueryState.FAILED == state || QueryState.PARTIALLY_FAILED == state) {
                        throw new IllegalStateException("Query failed: " + queryStatus);
                    }
                    LOGGER.info("Query found with state {}: {}", state, query.getQueryId());
                    return QueryState.COMPLETED == state;
                } catch (QueryTrackerException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SleeperRow> getResults(Query query) {
        LOGGER.info("Loading results for query: {}", query.getQueryId());
        Schema schema = instance.getTablePropertiesByDeployedName(query.getTableName()).orElseThrow().getSchema();
        String bucketName = instance.getInstanceProperties().get(QUERY_RESULTS_BUCKET);
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(
                request -> request.bucket(bucketName).prefix("query-" + query.getQueryId()));
        return response.contents().stream()
                .flatMap(object -> ReadRecordsFromS3.getRecords(bucketName, object.key(), schema, clients.createHadoopConf()))
                .toList();
    }
}
