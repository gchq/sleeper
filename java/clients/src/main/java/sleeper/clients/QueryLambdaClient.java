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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.output.ResultsOutputConstants;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.QueryTrackerException;
import sleeper.query.tracker.TrackedQuery;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Runs queries by sending them to an SQS queue which will trigger a lambda to
 * execute the query.
 */
public class QueryLambdaClient extends QueryCommandLineClient {
    private final AmazonSQS sqsClient;
    private final DynamoDBQueryTracker queryTracker;
    private Map<String, String> resultsPublisherConfig;
    private final String queryQueueUrl;
    private final QuerySerDe querySerDe;

    public QueryLambdaClient(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, AmazonSQS sqsClient, InstanceProperties instanceProperties) {
        super(s3Client, dynamoDBClient, instanceProperties);
        this.sqsClient = sqsClient;
        this.queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);
        this.queryQueueUrl = instanceProperties.get(QUERY_QUEUE_URL);
        this.querySerDe = new QuerySerDe(new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient));
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
        sqsClient.sendMessage(queryQueueUrl, querySerDe.toJson(
                query.withResultsPublisherConfig(resultsPublisherConfig)));
    }

    public static void main(String[] args) throws StateStoreException, InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            QueryLambdaClient queryLambdaClient = new QueryLambdaClient(s3Client, dynamoDBClient, sqsClient, instanceProperties);
            queryLambdaClient.run();
        } finally {
            s3Client.shutdown();
            sqsClient.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
