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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.model.output.ResultsOutputConstants;
import sleeper.query.model.output.SQSResultsOutput;
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.TrackedQuery;
import sleeper.query.tracker.exception.QueryTrackerException;
import sleeper.core.statestore.StateStoreException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

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

    public QueryLambdaClient(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, AmazonSQS sqsClient, InstanceProperties instanceProperties) {
        super(s3Client, instanceProperties);
        this.sqsClient = sqsClient;
        this.queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDB);
        this.queryQueueUrl = instanceProperties.get(QUERY_QUEUE_URL);
        this.querySerDe = new QuerySerDe(new TablePropertiesProvider(s3Client, instanceProperties));
    }

    @Override
    protected void init(TableProperties tableProperties) throws StateStoreException {
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
    protected void runQueries(TableProperties tableProperties) {
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
        query.setResultsPublisherConfig(resultsPublisherConfig);
        sqsClient.sendMessage(queryQueueUrl, querySerDe.toJson(query));
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        AmazonSQS amazonSQS = AmazonSQSClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        QueryLambdaClient queryLambdaClient = new QueryLambdaClient(amazonS3, dynamoDB, amazonSQS, instanceProperties);
        queryLambdaClient.run();
    }
}
