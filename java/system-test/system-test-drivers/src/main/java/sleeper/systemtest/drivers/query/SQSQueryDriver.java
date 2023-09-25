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
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.TrackedQuery;
import sleeper.query.tracker.exception.QueryTrackerException;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.ReadRecordsFromS3;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class SQSQueryDriver implements QueryDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQSQueryDriver.class);

    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private final String queueUrl;
    private final String resultsBucket;
    private final Schema schema;
    private final QuerySerDe querySerDe;
    private final DynamoDBQueryTracker queryTracker;
    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(2), Duration.ofMinutes(1));

    public SQSQueryDriver(SleeperInstanceContext instance,
                          AmazonSQS sqsClient,
                          AmazonDynamoDB dynamoDBClient,
                          AmazonS3 s3Client) {
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        this.queueUrl = instance.getInstanceProperties().get(QUERY_QUEUE_URL);
        this.resultsBucket = instance.getInstanceProperties().get(QUERY_RESULTS_BUCKET);
        this.schema = instance.getTableProperties().getSchema();
        this.querySerDe = new QuerySerDe(Map.of(instance.getTableName(), schema));
        this.queryTracker = new DynamoDBQueryTracker(instance.getInstanceProperties(), dynamoDBClient);
    }

    public List<Record> run(Query query) throws InterruptedException {
        sqsClient.sendMessage(queueUrl, querySerDe.toJson(query));
        waitForQuery(query.getQueryId());
        return s3Client.listObjects(resultsBucket, "query-" + query.getQueryId())
                .getObjectSummaries().stream()
                .flatMap(object -> ReadRecordsFromS3.getRecords(schema, object))
                .collect(Collectors.toUnmodifiableList());
    }

    private void waitForQuery(String queryId) throws InterruptedException {
        poll.pollUntil("query is finished", () -> {
            try {
                TrackedQuery queryStatus = queryTracker.getStatus(queryId);
                if (queryStatus == null) {
                    LOGGER.info("Query not found yet, retrying...");
                    return false;
                }
                QueryState state = queryStatus.getLastKnownState();
                if (QueryState.FAILED == state || QueryState.PARTIALLY_FAILED == state) {
                    throw new IllegalStateException("Query failed: " + queryStatus);
                }
                LOGGER.info("Query found with state: {}", state);
                return QueryState.COMPLETED == state;
            } catch (QueryTrackerException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
