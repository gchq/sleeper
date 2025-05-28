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
package sleeper.clients.query;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutputConstants;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.TrackedQuery;
import sleeper.query.runnerv2.output.SQSResultsOutput;
import sleeper.query.runnerv2.tracker.DynamoDBQueryTracker;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Runs queries by sending them to an SQS queue which will trigger a lambda to
 * execute the query.
 */
public class QueryLambdaClient extends QueryCommandLineClient {
    private final SqsClient sqsClient;
    private final DynamoDBQueryTracker queryTracker;
    private Map<String, String> resultsPublisherConfig;
    private final String queryQueueUrl;
    private final QuerySerDe querySerDe;

    public QueryLambdaClient(S3Client s3Client, DynamoDbClient dynamoClient, SqsClient sqsClient, InstanceProperties instanceProperties) {
        super(s3Client, dynamoClient, instanceProperties);
        this.sqsClient = sqsClient;
        this.queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
        this.queryQueueUrl = instanceProperties.get(QUERY_QUEUE_URL);
        this.querySerDe = new QuerySerDe(S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient));
    }

    @Override
    protected void init(TableProperties tableProperties) {
        // No-op
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        System.out.println("Submitting query with id " + query.getQueryId());
        submitQuery(query);
        long sleepTime = 1000L;
        try {
            QueryState state;
            int count = 0;
            while (true) {
                System.out.println("Polling query tracker");
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
                System.out.println("Sleeping for " + (sleepTime / 1000) + " seconds");
                Thread.sleep(sleepTime);
            }
            System.out.println("Finished query processing with final state of: " + state);
        } catch (QueryTrackerException | InterruptedException e) {
            System.out.println("Failed to get status");
            e.printStackTrace();
        }
    }

    @Override
    protected void runQueries(TableProperties tableProperties) throws InterruptedException {
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
        resultsPublisherConfig = new HashMap<>();
        while (true) {
            System.out.println("Send output to S3 results bucket (s) or SQS (q)?");
            String type = scanner.nextLine();
            if ("".equals(type)) {
                break;
            }
            if (!type.equalsIgnoreCase("s") && !type.equalsIgnoreCase("q")) {
                continue;
            }
            if (type.equalsIgnoreCase("s")) {
                // Nothing to do - empty resultsPublisherConfig will cause the
                // results to be published to S3.
                System.out.println("Results will be published to S3 bucket " + getInstanceProperties().get(QUERY_RESULTS_BUCKET));
            } else {
                resultsPublisherConfig.put(ResultsOutputConstants.DESTINATION, SQSResultsOutput.SQS);
                System.out.println("Results will be published to SQS queue " + getInstanceProperties().get(QUERY_RESULTS_QUEUE_URL));
            }
            break;
        }
        super.runQueries(tableProperties);
    }

    public void submitQuery(Query query) {
        sqsClient.sendMessage(request -> request.queueUrl(queryQueueUrl)
                .messageBody(querySerDe.toJson(
                        query.withResultsPublisherConfig(resultsPublisherConfig))));
    }

    public static void main(String[] args) throws InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                SqsClient sqsClient = SqsClient.create()) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            QueryLambdaClient queryLambdaClient = new QueryLambdaClient(s3Client, dynamoClient, sqsClient, instanceProperties);
            queryLambdaClient.run();
        }
    }
}
