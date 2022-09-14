/*
 * Copyright 2022 Crown Copyright
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
package sleeper.query.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.JsonParseException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.query.QueryException;
import sleeper.query.executor.QueryExecutor;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.model.output.ResultsOutputConstants;
import sleeper.query.model.output.ResultsOutputInfo;
import sleeper.query.model.output.S3ResultsOutput;
import sleeper.query.model.output.SQSResultsOutput;
import sleeper.query.model.output.WebSocketResultsOutput;
import sleeper.query.recordretrieval.LeafPartitionQueryExecutor;
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryStatusReportListeners;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS;

/**
 * A lambda that is triggered when a serialised query arrives on an SQS queue. It executes the request using a
 * {@link QueryExecutor} and publishes the results to either SQS or S3 based on the configuration of the query.
 * It caches the mapping from partitions to files in those partitions in a variable. This is reused by subsequent
 * calls to the lambda if the AWS runtime chooses to reuse the instance.
 */
@SuppressWarnings("unused")
public class SqsQueryProcessorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueryProcessorLambda.class);

    private final ExecutorService executorService;
    private long lastUpdateTime;
    private InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private TablePropertiesProvider tablePropertiesProvider;
    private StateStoreProvider stateStoreProvider;
    private ObjectFactory objectFactory;
    private Configuration queryConfiguration;
    private DynamoDBQueryTracker queryTracker;
    private final Map<String, QueryExecutor> queryExecutorCache = new HashMap<>();
    private QuerySerDe serde;

    public SqsQueryProcessorLambda() throws ObjectFactoryException {
        this(AmazonS3ClientBuilder.defaultClient(), AmazonSQSClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SqsQueryProcessorLambda(AmazonS3 s3Client, AmazonSQS sqsClient, AmazonDynamoDB dynamoClient, String configBucket) throws ObjectFactoryException {
        this.s3Client = s3Client;
        this.sqsClient = sqsClient;
        this.dynamoClient = dynamoClient;
        this.executorService = Executors.newFixedThreadPool(10);
        updateProperties(configBucket);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        try {
            updateStateIfNecessary();
        } catch (ObjectFactoryException e) {
            throw new RuntimeException("ObjectFactoryException updating state", e);
        }

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            LOGGER.info("Received message with body {}", message.getBody());
            Query query;
            try {
                query = serde.fromJson(message.getBody());
                LOGGER.info("Deserialised message to query {}", query);
            } catch (JsonParseException e) {
                LOGGER.error("JSONParseException deserialsing query from JSON {}", message.getBody());
                continue;
            }
            processQuery(query);
        }
        return null;
    }

    private void updateStateIfNecessary() throws ObjectFactoryException {
        double timeSinceLastUpdatedInSeconds = (System.currentTimeMillis() - lastUpdateTime) / 1000.0;
        int stateRefreshingPeriod = instanceProperties.getInt(QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS);
        if (timeSinceLastUpdatedInSeconds > stateRefreshingPeriod) {
            LOGGER.info("Mapping of partition to files was last updated {} seconds ago, so refreshing", timeSinceLastUpdatedInSeconds);
            updateProperties(instanceProperties.get(CONFIG_BUCKET));
        }
    }

    private void updateProperties(String configBucket) throws ObjectFactoryException {
        // Refresh properties and caches
        if (null == configBucket) {
            LOGGER.error("Config Bucket was null. Was an environment variable missing?");
            throw new RuntimeException("Error: can't find S3 bucket from environment variable");
        }
        instanceProperties = new InstanceProperties();
        try {
            instanceProperties.loadFromS3(s3Client, configBucket);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load instance properties");
        }
        queryConfiguration = HadoopConfigurationProvider.getConfigurationForQueryLambdas(instanceProperties);
        if (null == objectFactory) {
            objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");
        }
        queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForQueryLambdas(instanceProperties);
        tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        serde = new QuerySerDe(this.tablePropertiesProvider);
        stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties, conf);
        queryExecutorCache.clear();
        lastUpdateTime = System.currentTimeMillis();
    }

    private void processQuery(Query query) {
        QueryStatusReportListeners queryTrackers = QueryStatusReportListeners.fromConfig(query.getStatusReportDestinations());
        if (queryTracker != null) {
            queryTrackers.add(queryTracker);
        }

        CloseableIterator<Record> results;
        try {
            queryTrackers.queryInProgress(query);
            if (query instanceof LeafPartitionQuery) {
                results = processLeafPartitionQuery((LeafPartitionQuery) query, tablePropertiesProvider.getTableProperties(query.getTableName()));
            } else if (query instanceof Query) {
                results = processRangeQuery(query, queryTrackers);
            } else {
                throw new IllegalArgumentException("Found query of unknown type " + query.getClass());
            }
            if (null != results) {
                publishResults(results, query, queryTrackers);
            }
        } catch (StateStoreException | QueryException e) {
            LOGGER.error("Exception thrown executing query", e);
            queryTrackers.queryFailed(query, e);
        }
    }

    private CloseableIterator<Record> processRangeQuery(Query query, QueryStatusReportListeners queryTrackers) throws StateStoreException, QueryException {
        // Split query over leaf partitions
        if (!queryExecutorCache.containsKey(query.getTableName())) {
            TableProperties tableProperties = tablePropertiesProvider.getTableProperties(query.getTableName());
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            QueryExecutor queryExecutor = new QueryExecutor(objectFactory, tableProperties, stateStore, queryConfiguration, executorService);
            queryExecutor.init();
            queryExecutorCache.put(query.getTableName(), queryExecutor);
        }
        QueryExecutor queryExecutor = queryExecutorCache.get(query.getTableName());
        List<LeafPartitionQuery> subQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        if (subQueries.size() > 1) {
            // Put these subqueries back onto the queue so that they
            // can be processed independently
            String sqsQueryQueueURL = instanceProperties.get(QUERY_QUEUE_URL);
            for (LeafPartitionQuery subQuery : subQueries) {
                String serialisedQuery = new QuerySerDe(tablePropertiesProvider).toJson(subQuery);
                sqsClient.sendMessage(sqsQueryQueueURL, serialisedQuery);
            }
            queryTrackers.subQueriesCreated(query, subQueries);
            LOGGER.info("Submitted {} subqueries to queue", subQueries.size());
            return null;
        } else if (subQueries.isEmpty()) {
            LOGGER.error("Query led to no sub queries");
            /*
             * Not setting the state to failed because the table may not have contained any data.
             */
            queryTrackers.queryCompleted(query, new ResultsOutputInfo(0, Collections.emptyList()));
            return null;
        } else {
            // If only 1 subquery then execute now
            return queryExecutor.execute(query);
        }
    }

    private CloseableIterator<Record> processLeafPartitionQuery(LeafPartitionQuery leafPartitionQuery, TableProperties tableProperties)
            throws QueryException {
        LeafPartitionQueryExecutor leafPartitionQueryExecutor = new LeafPartitionQueryExecutor(executorService, objectFactory, queryConfiguration, tableProperties);
        return leafPartitionQueryExecutor.getRecords(leafPartitionQuery);
    }

    private void publishResults(CloseableIterator<Record> results, Query query, QueryStatusReportListeners queryTrackers) {
        Schema schema = tablePropertiesProvider.getTableProperties(query.getTableName()).getSchema();

        try {
            ResultsOutputInfo outputInfo;
            if (null == query.getResultsPublisherConfig() || query.getResultsPublisherConfig().isEmpty()) {
                outputInfo = new S3ResultsOutput(instanceProperties, schema, new HashMap<>()).publish(query, results);
            } else if (SQSResultsOutput.SQS.equals(query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION))) {
                outputInfo = new SQSResultsOutput(instanceProperties, sqsClient, schema, query.getResultsPublisherConfig()).publish(query, results);
            } else if (S3ResultsOutput.S3.equals(query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION))) {
                outputInfo = new S3ResultsOutput(instanceProperties, schema, query.getResultsPublisherConfig()).publish(query, results);
            } else if (WebSocketResultsOutput.DESTINATION_NAME.equals(query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION))) {
                outputInfo = new WebSocketResultsOutput(query.getResultsPublisherConfig()).publish(query, results);
            } else {
                LOGGER.info("Unknown results publisher from config " + query.getResultsPublisherConfig());
                outputInfo = new ResultsOutputInfo(0, Collections.emptyList(), new IOException("Unknown results publisher from config " + query.getResultsPublisherConfig()));
            }

            queryTrackers.queryCompleted(query, outputInfo);
        } catch (Exception e) {
            LOGGER.error("Error publishing results", e);
            queryTrackers.queryFailed(query, e);
        }
    }

}
