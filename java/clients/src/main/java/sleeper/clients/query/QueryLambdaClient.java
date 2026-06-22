/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.query;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.api.QuerySender;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.table.TableIndex;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Runs queries by sending them to an SQS queue which will trigger a lambda to
 * execute the query.
 */
public class QueryLambdaClient extends QueryCommandLineClient {
    private final QuerySender querySender;
    private final QueryTrackerStore queryTracker;
    private Map<String, String> resultsPublisherConfig;

    public QueryLambdaClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            QuerySender querySender, QueryTrackerStore queryTracker, ConsoleInput in, ConsoleOutput out) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out);
        this.querySender = querySender;
        this.queryTracker = queryTracker;
    }

    @Override
    protected void init(TableProperties tableProperties) {
        // No-op
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        out.println("Submitting query with id " + query.getQueryId());
        submitQuery(query);
        long sleepTime = 1000L;
        try {
            QueryState state;
            int count = 0;
            while (true) {
                out.println("Polling query tracker");
                TrackedQuery trackedQuery = queryTracker.getStatus(query.getQueryId());
                if (trackedQuery != null) {
                    state = trackedQuery.getLastKnownState();
                    if (!state.equals(QueryState.IN_PROGRESS)) {
                        break;
                    }
                }
                count++;
                if (count > 20) {
                    sleepTime = 5000L;
                } else if (count > 10) {
                    sleepTime = 2000L;
                }
                out.println("Sleeping for " + (sleepTime / 1000) + " seconds");
                Thread.sleep(sleepTime);
            }
            out.println("Finished query processing with final state of: " + state);
        } catch (QueryTrackerException | InterruptedException e) {
            out.println("Failed to get status");
            out.printStackTrace(e);
        }
    }

    @Override
    protected void runQueries(TableProperties tableProperties, String sqlQuery) throws InterruptedException {
        resultsPublisherConfig = new HashMap<>();
        while (true) {
            String type = in.promptLine("Send output to S3 results bucket (s) or SQS (q)?");
            if ("".equals(type)) {
                break;
            }
            if ("s".equalsIgnoreCase(type)) {
                // Nothing to do - empty resultsPublisherConfig will cause the
                // results to be published to S3.
                out.println("Results will be published to S3 bucket " + getInstanceProperties().get(QUERY_RESULTS_BUCKET));
            } else if ("q".equals(type)) {
                resultsPublisherConfig.put(ResultsOutput.DESTINATION, SQSResultsOutput.SQS);
                out.println("Results will be published to SQS queue " + getInstanceProperties().get(QUERY_RESULTS_QUEUE_URL));
            } else {
                continue;
            }
            break;
        }
        super.runQueries(tableProperties, sqlQuery);
    }

    public void submitQuery(Query query) {
        querySender.sendQuery(query.withResultsPublisherConfig(resultsPublisherConfig));
    }

    public static void main(String[] args) throws InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        String instanceId = args[0];

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                SqsClient sqsClient = SqsClient.create();
                StsClient stsClient = StsClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
            TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, tableIndex, s3Client);
            QueryLambdaClient queryLambdaClient = new QueryLambdaClient(
                    instanceProperties, tableIndex, tablePropertiesProvider,
                    QuerySender.toSqs(instanceProperties, tablePropertiesProvider, sqsClient),
                    new DynamoDBQueryTracker(instanceProperties, dynamoClient),
                    ConsoleInput.stdIn(), ConsoleOutput.stdOut());
            queryLambdaClient.run();
        }
    }
}
