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

import sleeper.clients.api.QueryResultsReader;
import sleeper.clients.api.QuerySender;
import sleeper.clients.api.StreamingQueryResultsIterator;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIndex;
import sleeper.core.util.PollWithRetries;
import sleeper.parquet.utils.TableHadoopConfigurationProvider;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Runs queries by sending them to an SQS queue which will trigger a lambda to
 * execute the query.
 */
public class QueryLambdaClient extends QueryCommandLineClient {
    private static final String S3_PROMPT_RESPONSE = "s";
    private static final String SQS_PROMPT_RESPONSE = "q";
    private static final String CONSOLE_PROMPT_RESPONSE = "c";
    private static final String WHEN_FINISHED_PROMPT_RESPONSE = "f";
    private static final String AS_SUBQUERIES_FINISH_PROMPT_RESPONSE = "a";
    private static final PollWithRetries STREAM_RESULTS_POLL = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(2), Duration.ofMinutes(15));

    private final QuerySender querySender;
    private final QueryTrackerStore queryTracker;
    private final QueryResultsReader queryResultsReader;
    private Map<String, String> resultsPublisherConfig;
    private boolean printResultsToConsole;
    private boolean streamResultsToConsole;

    public QueryLambdaClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            QuerySender querySender, QueryTrackerStore queryTracker, QueryResultsReader queryResultsReader,
            ConsoleInput in, ConsoleOutput out) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out);
        this.querySender = querySender;
        this.queryTracker = queryTracker;
        this.queryResultsReader = queryResultsReader;
    }

    @Override
    protected void init(TableProperties tableProperties) {
        // No-op
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        String queryId = query.getQueryId();
        out.println("Submitting query with id " + queryId);

        if (streamResultsToConsole) {
            submitQueryAndStreamResults(tableProperties, query);
            return;
        }

        submitQuery(query);

        try {
            QueryState state;
            int count = 0;
            TrackedQuery trackedQuery;

            while (true) {
                out.println("Polling query tracker");

                trackedQuery = queryTracker.getStatus(queryId);
                if (trackedQuery != null) {
                    state = trackedQuery.getLastKnownState();
                    if (!state.equals(QueryState.IN_PROGRESS)) {
                        break;
                    }
                    describeProgress(trackedQuery).ifPresent(out::println);
                }
                count++;

                long sleepTime = sleepTimeForPollCount(count);
                out.println("Sleeping for " + (sleepTime / 1000) + " seconds");
                Thread.sleep(sleepTime);
            }

            out.println("Finished query processing with final state of: " + state);
            if (trackedQuery.getRowCount() != null) {
                out.println("Query returned " + trackedQuery.getRowCount() + " rows");
            }
            if (printResultsToConsole && state == QueryState.COMPLETED && returnedRows(trackedQuery)) {
                printResults(tableProperties, query);
            }
        } catch (QueryTrackerException | InterruptedException e) {
            out.println("Failed to get status");
            out.printStackTrace(e);
        }
    }

    private static boolean returnedRows(TrackedQuery trackedQuery) {
        // An optimisation to avoid reading files if no results were returned.
        return trackedQuery.getRowCount() == null || trackedQuery.getRowCount() > 0;
    }

    private void printResults(TableProperties tableProperties, Query query) {
        Schema schema = tableProperties.getSchema();
        try (CloseableIterator<Row> rows = queryResultsReader.getResults(query)) {
            out.println("Returned Rows:");
            while (rows.hasNext()) {
                out.println(rows.next().toString(schema));
            }
        } catch (RuntimeException e) {
            out.println("Encountered an error while reading results of query " + query.getQueryId());
            out.printStackTrace(e);
        } catch (IOException e) {
            out.println("Encountered an error while closing iterator for query " + query.getQueryId());
            out.printStackTrace(e);
        }
    }

    /**
     * Submits a query and prints its results to the console as they are published, without waiting for the whole
     * query to finish.
     *
     * @param tableProperties the properties of the table the query runs against
     * @param query           the query
     */
    private void submitQueryAndStreamResults(TableProperties tableProperties, Query query) {
        Schema schema = tableProperties.getSchema();
        submitQuery(query);
        long count = 0;
        try (CloseableIterator<Row> rows = new StreamingQueryResultsIterator(
                query.getTableName(), query.getQueryId(), queryResultsReader,
                () -> getTrackedQueryOrNull(query.getQueryId()), STREAM_RESULTS_POLL)) {
            out.println("Returned Rows:");
            while (rows.hasNext()) {
                out.println(rows.next().toString(schema));
                count++;
            }
            out.println("Query finished, returned " + count + " rows");
        } catch (RuntimeException e) {
            out.println("Encountered an error while streaming results of query " + query.getQueryId()
                    + ", printed " + count + " rows so far");
            out.printStackTrace(e);
        } catch (IOException e) {
            out.println("Encountered an error while closing iterator for query " + query.getQueryId());
            out.printStackTrace(e);
        }
    }

    private TrackedQuery getTrackedQueryOrNull(String queryId) {
        try {
            return queryTracker.getStatus(queryId);
        } catch (QueryTrackerException e) {
            throw new RuntimeException("Failed reading status of query " + queryId + " from the query tracker", e);
        }
    }

    @Override
    protected void runQueries(TableProperties tableProperties) throws InterruptedException {
        resultsPublisherConfig = new HashMap<>();
        printResultsToConsole = false;
        streamResultsToConsole = false;
        boolean chosen = false;

        while (!chosen) {
            String type = in.promptLine("Send output to S3 results bucket (s), SQS (q), or the console (c)? [s]");

            chosen = switch (type.toLowerCase(Locale.ROOT)) {
                case "", S3_PROMPT_RESPONSE -> {
                    out.println("Results will be published to S3 bucket "
                            + getInstanceProperties().get(QUERY_RESULTS_BUCKET));

                    yield true;
                }

                case SQS_PROMPT_RESPONSE -> {
                    out.println("Results will be published to SQS queue "
                            + getInstanceProperties().get(QUERY_RESULTS_QUEUE_URL));

                    resultsPublisherConfig.put(ResultsOutput.DESTINATION, SQSResultsOutput.SQS);

                    yield true;
                }

                case CONSOLE_PROMPT_RESPONSE -> {
                    out.println("Results will be published to S3 bucket "
                            + getInstanceProperties().get(QUERY_RESULTS_BUCKET)
                            + ", then printed to the console");

                    printResultsToConsole = true;
                    streamResultsToConsole = promptStreamResults();

                    yield true;
                }

                default -> false;
            };
        }

        super.runQueries(tableProperties);
    }

    private boolean promptStreamResults() {
        while (true) {
            String type = in.promptLine("Print results when the query has finished (f), " +
                    "or as each subquery finishes (a)? [f]");

            switch (type.toLowerCase(Locale.ROOT)) {
                case "", WHEN_FINISHED_PROMPT_RESPONSE -> {
                    out.println("Results will be printed when the query has finished");
                    return false;
                }
                case AS_SUBQUERIES_FINISH_PROMPT_RESPONSE -> {
                    out.println("Results will be printed as each subquery finishes");
                    return true;
                }
                default -> {
                    // Prompt again
                }
            }
        }
    }

    /**
     * Sends a query to the query SQS queue.
     *
     * @param query the query to submit
     */
    public void submitQuery(Query query) {
        querySender.sendQuery(query.withResultsPublisherConfig(resultsPublisherConfig));
    }

    /**
     * Describes the progress of a query that is still in progress. The counts are only tracked
     * on a parent query that was split into sub-queries.
     *
     * @param  trackedQuery the status of the query held in the query tracker
     * @return              a description of how many sub-queries have finished, if that is known
     */
    static Optional<String> describeProgress(TrackedQuery trackedQuery) {
        Long expected = trackedQuery.getExpectedSubQueryCount();
        Long finished = trackedQuery.getFinishedSubQueryCount();
        if (expected == null || finished == null) {
            return Optional.empty();
        }
        String progress = "Query in progress: " + finished + " of " + expected + " subqueries finished, "
                + trackedQuery.getRemainingSubQueryCount() + " remaining";
        Long rowCount = trackedQuery.getFinishedSubQueryRowCount();
        if (rowCount != null) {
            progress += ", " + rowCount + " rows output so far";
        }
        return Optional.of(progress);
    }

    private static long sleepTimeForPollCount(int count) {
        if (count > 20) {
            return 5000L;
        } else if (count > 10) {
            return 2000L;
        }
        return 1000L;
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
                    QueryResultsReader.fromS3(instanceProperties, tablePropertiesProvider,
                            TableHadoopConfigurationProvider.forClient(instanceProperties)),
                    ConsoleInput.stdIn(), ConsoleOutput.stdOut());
            queryLambdaClient.run();
        }
    }
}
