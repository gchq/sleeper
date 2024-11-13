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
package sleeper.query.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.UserDefinedInstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.query.runner.recordretrieval.QueryExecutor;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.runner.tracker.QueryStatusReportListeners;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class SqsQueryProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueryProcessor.class);
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
        objectFactory = new S3UserJarsLoader(instanceProperties, builder.s3Client, "/tmp").buildObjectFactory();
        queryTracker = new DynamoDBQueryTracker(instanceProperties, builder.dynamoClient);
        // The following Configuration is only used in StateStoreProvider for reading from S3 if the S3StateStore is used,
        // so use the standard Configuration rather than the one for query lambdas which is specific to the table.
        Configuration confForStateStore = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, builder.s3Client, builder.dynamoClient, confForStateStore);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void processQuery(QueryOrLeafPartitionQuery query) {
        QueryStatusReportListeners queryTrackers = QueryStatusReportListeners.fromConfig(
                query.getProcessingConfig().getStatusReportDestinations());
        queryTrackers.add(queryTracker);
        try {
            TableProperties tableProperties = query.getTableProperties(tablePropertiesProvider);
            Query parentQuery = query.asParentQuery();
            queryTrackers.queryInProgress(parentQuery);
            processRangeQuery(parentQuery, tableProperties, queryTrackers);
        } catch (StateStoreException | QueryException e) {
            LOGGER.error("Exception thrown executing query", e);
            query.reportFailed(queryTrackers, e);
        }
    }

    private void processRangeQuery(Query query, TableProperties tableProperties, QueryStatusReportListeners queryTrackers) throws StateStoreException, QueryException {
        QueryExecutor queryExecutor = queryExecutorCache.computeIfAbsent(query.getTableName(), tableName -> {
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            Configuration conf = getConfiguration(tableProperties);
            return new QueryExecutor(objectFactory, tableProperties, stateStore,
                    new LeafPartitionRecordRetrieverImpl(executorService, conf));
        });

        queryExecutor.initIfNeeded(Instant.now());
        List<LeafPartitionQuery> subQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        if (subQueries.isEmpty()) {
            LOGGER.error("Query led to no sub queries");
            /*
             * Not setting the state to failed because the table may not have contained any data.
             */
            queryTrackers.queryCompleted(query, new ResultsOutputInfo(0, Collections.emptyList()));
            return;
        }

        // Put these subqueries on to the leaf partition query queue so they can be processed independently
        String sqsLeafPartitionQueryQueueURL = instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_URL);
        for (LeafPartitionQuery subQuery : subQueries) {
            String serialisedQuery = new QuerySerDe(tablePropertiesProvider).toJson(subQuery);
            sqsClient.sendMessage(sqsLeafPartitionQueryQueueURL, serialisedQuery);
        }
        queryTrackers.subQueriesCreated(query, subQueries);
        LOGGER.info("Submitted {} subqueries to queue", subQueries.size());
    }

    private Configuration getConfiguration(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        if (!configurationCache.containsKey(tableName)) {
            Configuration conf = HadoopConfigurationProvider.getConfigurationForQueryLambdas(instanceProperties, tableProperties);
            configurationCache.put(tableName, conf);
        }
        return configurationCache.get(tableName);
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
