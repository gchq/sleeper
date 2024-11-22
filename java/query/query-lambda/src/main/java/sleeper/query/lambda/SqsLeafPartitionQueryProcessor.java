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
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.UserDefinedInstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputConstants;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.recordretrieval.LeafPartitionQueryExecutor;
import sleeper.query.runner.output.NoResultsOutput;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.output.WebSocketResultsOutput;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.runner.tracker.QueryStatusReportListeners;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.query.runner.output.NoResultsOutput.NO_RESULTS_OUTPUT;

public class SqsLeafPartitionQueryProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsLeafPartitionQueryProcessor.class);
    private static final UserDefinedInstanceProperty EXECUTOR_POOL_THREADS = QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS;

    private final ExecutorService executorService;
    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final ObjectFactory objectFactory;
    private final DynamoDBQueryTracker queryTracker;
    private final Map<String, Configuration> configurationCache = new HashMap<>();

    private SqsLeafPartitionQueryProcessor(Builder builder) throws ObjectFactoryException {
        sqsClient = builder.sqsClient;
        instanceProperties = builder.instanceProperties;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        executorService = Executors.newFixedThreadPool(instanceProperties.getInt(EXECUTOR_POOL_THREADS));
        objectFactory = new S3UserJarsLoader(instanceProperties, builder.s3Client, "/tmp").buildObjectFactory();
        queryTracker = new DynamoDBQueryTracker(instanceProperties, builder.dynamoClient);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void processQuery(LeafPartitionQuery leafPartitionQuery) {
        QueryOrLeafPartitionQuery query = new QueryOrLeafPartitionQuery(leafPartitionQuery);
        QueryStatusReportListeners queryTrackers = QueryStatusReportListeners.fromConfig(
                leafPartitionQuery.getProcessingConfig().getStatusReportDestinations());
        queryTrackers.add(queryTracker);

        try {
            TableProperties tableProperties = query.getTableProperties(tablePropertiesProvider);
            queryTrackers.queryInProgress(leafPartitionQuery);
            Configuration conf = getConfiguration(tableProperties);
            LeafPartitionQueryExecutor leafPartitionQueryExecutor = new LeafPartitionQueryExecutor(
                    objectFactory, tableProperties, new LeafPartitionRecordRetrieverImpl(executorService, conf));
            CloseableIterator<Record> results = leafPartitionQueryExecutor.getRecords(leafPartitionQuery);
            publishResults(results, query, tableProperties, queryTrackers);
        } catch (QueryException e) {
            LOGGER.error("Exception thrown executing leaf partition query {}", query.getQueryId(), e);
            query.reportFailed(queryTrackers, e);
        }
    }

    private Configuration getConfiguration(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        if (!configurationCache.containsKey(tableName)) {
            Configuration conf = HadoopConfigurationProvider.getConfigurationForQueryLambdas(instanceProperties, tableProperties);
            configurationCache.put(tableName, conf);
        }
        return configurationCache.get(tableName);
    }

    private void publishResults(CloseableIterator<Record> results, QueryOrLeafPartitionQuery query, TableProperties tableProperties, QueryStatusReportListeners queryTrackers) {
        try {
            Map<String, String> resultsPublisherConfig = query.getProcessingConfig().getResultsPublisherConfig();
            ResultsOutputInfo outputInfo = getResultsOutput(tableProperties, resultsPublisherConfig)
                    .publish(query, results);

            query.reportCompleted(queryTrackers, outputInfo);
        } catch (Exception e) {
            LOGGER.error("Error publishing results", e);
            query.reportFailed(queryTrackers, e);
        }
    }

    private ResultsOutput getResultsOutput(TableProperties tableProperties, Map<String, String> resultsPublisherConfig) {
        if (null == resultsPublisherConfig || resultsPublisherConfig.isEmpty()) {
            return new S3ResultsOutput(instanceProperties, tableProperties, new HashMap<>());
        }
        String destination = resultsPublisherConfig.get(ResultsOutputConstants.DESTINATION);
        if (SQSResultsOutput.SQS.equals(destination)) {
            return new SQSResultsOutput(instanceProperties, sqsClient, tableProperties.getSchema(), resultsPublisherConfig);
        } else if (S3ResultsOutput.S3.equals(destination)) {
            return new S3ResultsOutput(instanceProperties, tableProperties, resultsPublisherConfig);
        } else if (WebSocketResultsOutput.DESTINATION_NAME.equals(destination)) {
            return new WebSocketResultsOutput(tableProperties.getSchema(), resultsPublisherConfig);
        } else if (NO_RESULTS_OUTPUT.equals(destination)) {
            return new NoResultsOutput();
        } else {
            LOGGER.info("Unknown results publisher from config {}", resultsPublisherConfig);
            return (query, results) -> new ResultsOutputInfo(0, Collections.emptyList(),
                    new IOException("Unknown results publisher from config " + query.getProcessingConfig().getResultsPublisherConfig()));
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

        public SqsLeafPartitionQueryProcessor build() throws ObjectFactoryException {
            return new SqsLeafPartitionQueryProcessor(this);
        }
    }
}
