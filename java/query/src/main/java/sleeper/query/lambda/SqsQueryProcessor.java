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
package sleeper.query.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
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
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS;

public class SqsQueryProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueryProcessorLambda.class);
    private static final UserDefinedInstanceProperty EXECUTOR_POOL_THREADS = QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS;

    private final ExecutorService executorService;
    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final ObjectFactory objectFactory;
    private final DynamoDBQueryTracker queryTracker;
    private final Map<String, QueryExecutor> queryExecutorCache = new HashMap<>();
    private final Map<String, Configuration> configurationCache = new HashMap<>();

    private SqsQueryProcessor(Builder builder) throws ObjectFactoryException {
        sqsClient = builder.sqsClient;
        instanceProperties = builder.instanceProperties;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        executorService = Executors.newFixedThreadPool(instanceProperties.getInt(EXECUTOR_POOL_THREADS));
        objectFactory = new ObjectFactory(instanceProperties, builder.s3Client, "/tmp");
        queryTracker = new DynamoDBQueryTracker(instanceProperties, builder.dynamoClient);
        // The following Configuration is only used in StateStoreProvider for reading from S3 if the S3StateStore is used,
        // so use the standard Configuration rather than the one for query lambdas which is specific to the table.
        Configuration confForStateStore = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        stateStoreProvider = new StateStoreProvider(builder.dynamoClient, instanceProperties, confForStateStore);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void processQuery(Query query) {
        QueryStatusReportListeners queryTrackers = QueryStatusReportListeners.fromConfig(query.getStatusReportDestinations());
        queryTrackers.add(queryTracker);

        CloseableIterator<Record> results;
        try {
            queryTrackers.queryInProgress(query);
            TableProperties tableProperties = tablePropertiesProvider.getByName(query.getTableName());
            if (query instanceof LeafPartitionQuery) {
                results = processLeafPartitionQuery((LeafPartitionQuery) query);
            } else {
                results = processRangeQuery(query, queryTrackers);
            }
            if (null != results) {
                publishResults(results, query, tableProperties, queryTrackers);
            }
        } catch (StateStoreException | QueryException e) {
            LOGGER.error("Exception thrown executing query", e);
            queryTrackers.queryFailed(query, e);
        }
    }

    private CloseableIterator<Record> processRangeQuery(Query query, QueryStatusReportListeners queryTrackers) throws StateStoreException, QueryException {
        // If the cache needs refreshing remove to allow for a new in initialisation
        if (queryExecutorCache.get(query.getTableName()).cacheRefreshRequired()) {
            LOGGER.info("Refreshing Query Executor cache");
            queryExecutorCache.remove(query.getTableName());
        }
        // Split query over leaf partitions
        if (!queryExecutorCache.containsKey(query.getTableName())) {
            TableProperties tableProperties = tablePropertiesProvider.getByName(query.getTableName());
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            Configuration conf = getConfiguration(query.getTableName(), tableProperties);
            QueryExecutor queryExecutor = new QueryExecutor(objectFactory, tableProperties, stateStore, conf, executorService);
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

    private CloseableIterator<Record> processLeafPartitionQuery(LeafPartitionQuery leafPartitionQuery) throws QueryException {
        TableProperties tableProperties = tablePropertiesProvider.getByName(leafPartitionQuery.getTableName());
        Configuration conf = getConfiguration(leafPartitionQuery.getTableName(), tableProperties);
        LeafPartitionQueryExecutor leafPartitionQueryExecutor = new LeafPartitionQueryExecutor(executorService, objectFactory, conf, tableProperties);
        return leafPartitionQueryExecutor.getRecords(leafPartitionQuery);
    }

    private Configuration getConfiguration(String tableName, TableProperties tableProperties) {
        if (!configurationCache.containsKey(tableName)) {
            Configuration conf = HadoopConfigurationProvider.getConfigurationForQueryLambdas(instanceProperties, tableProperties);
            configurationCache.put(tableName, conf);
        }
        return configurationCache.get(tableName);
    }

    private void publishResults(CloseableIterator<Record> results, Query query, TableProperties tableProperties, QueryStatusReportListeners queryTrackers) {
        Schema schema = tablePropertiesProvider.getByName(query.getTableName()).getSchema();

        try {
            ResultsOutputInfo outputInfo;
            if (null == query.getResultsPublisherConfig() || query.getResultsPublisherConfig().isEmpty()) {
                outputInfo = new S3ResultsOutput(instanceProperties, tableProperties, new HashMap<>()).publish(query, results);
            } else if (SQSResultsOutput.SQS.equals(query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION))) {
                outputInfo = new SQSResultsOutput(instanceProperties, sqsClient, schema, query.getResultsPublisherConfig()).publish(query, results);
            } else if (S3ResultsOutput.S3.equals(query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION))) {
                outputInfo = new S3ResultsOutput(instanceProperties, tableProperties, query.getResultsPublisherConfig()).publish(query, results);
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

    public static final class Builder {
        private AmazonSQS sqsClient;
        private AmazonS3 s3Client;
        private AmazonDynamoDB dynamoClient;
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;

        private Builder() {
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder s3Client(AmazonS3 s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder dynamoClient(AmazonDynamoDB dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public SqsQueryProcessor build() throws ObjectFactoryException {
            return new SqsQueryProcessor(this);
        }
    }
}
