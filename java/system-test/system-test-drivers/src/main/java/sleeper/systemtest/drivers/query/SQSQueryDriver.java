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

package sleeper.systemtest.drivers.query;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.util.PollWithRetries;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.QueryTrackerException;
import sleeper.query.tracker.QueryTrackerStore;
import sleeper.query.tracker.TrackedQuery;
import sleeper.systemtest.drivers.util.ReadRecordsFromS3;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesSendAndWaitDriver;
import sleeper.systemtest.dsl.query.QuerySendAndWaitDriver;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class SQSQueryDriver implements QuerySendAndWaitDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQSQueryDriver.class);

    private final SystemTestInstanceContext instance;
    private final AmazonSQS sqsClient;
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonS3 s3Client;
    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(2), Duration.ofMinutes(1));
    private final SystemTestClients clients;

    public SQSQueryDriver(
            SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.sqsClient = clients.getSqs();
        this.dynamoDBClient = clients.getDynamoDB();
        this.s3Client = clients.getS3();
        this.clients = clients;
    }

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        return new QueryAllTablesSendAndWaitDriver(instance,
                new SQSQueryDriver(instance, clients));
    }

    @Override
    public void send(Query query) {
        sqsClient.sendMessage(
                instance.getInstanceProperties().get(QUERY_QUEUE_URL),
                new QuerySerDe(new SchemaLoaderFromInstanceContext(instance)).toJson(query));
    }

    @Override
    public void waitFor(Query query) {
        QueryTrackerStore queryTracker = new DynamoDBQueryTracker(instance.getInstanceProperties(), dynamoDBClient);
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
    public List<Record> getResults(Query query) {
        LOGGER.info("Loading results for query: {}", query.getQueryId());
        Schema schema = instance.getTablePropertiesByDeployedName(query.getTableName()).orElseThrow().getSchema();
        return s3Client.listObjects(
                instance.getInstanceProperties().get(QUERY_RESULTS_BUCKET),
                "query-" + query.getQueryId())
                .getObjectSummaries().stream()
                .flatMap(object -> ReadRecordsFromS3.getRecords(schema, object, clients.createHadoopConf()))
                .collect(Collectors.toUnmodifiableList());
    }
}
