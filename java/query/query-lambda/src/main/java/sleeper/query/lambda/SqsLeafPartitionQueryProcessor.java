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
package sleeper.query.lambda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.runner.output.NoResultsOutput;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.output.WebSocketOutput;
import sleeper.query.runner.output.WebSocketResultsOutput;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.runner.tracker.QueryStatusReportListeners;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static sleeper.query.runner.output.NoResultsOutput.NO_RESULTS_OUTPUT;

public class SqsLeafPartitionQueryProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsLeafPartitionQueryProcessor.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final LeafPartitionRowRetrieverProvider rowRetrieverProvider;
    private final SqsClient sqsClient;
    private final ObjectFactory objectFactory;
    private final DynamoDBQueryTracker queryTracker;

    private SqsLeafPartitionQueryProcessor(Builder builder) throws ObjectFactoryException {
        instanceProperties = builder.instanceProperties;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        rowRetrieverProvider = builder.rowRetrieverProvider;
        sqsClient = builder.sqsClient;
        objectFactory = new S3UserJarsLoader(instanceProperties, builder.s3Client, Path.of("/tmp")).buildObjectFactory();
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
            LeafPartitionQueryExecutor leafPartitionQueryExecutor = new LeafPartitionQueryExecutor(
                    objectFactory, tableProperties,
                    rowRetrieverProvider.getRowRetriever(tableProperties));
            CloseableIterator<Row> results = leafPartitionQueryExecutor.getRows(leafPartitionQuery);
            publishResults(results, query, tableProperties, queryTrackers);
        } catch (QueryException e) {
            LOGGER.error("Exception thrown executing leaf partition query {}", query.getQueryId(), e);
            query.reportFailed(queryTrackers, e);
        }
    }

    private void publishResults(CloseableIterator<Row> results, QueryOrLeafPartitionQuery query, TableProperties tableProperties, QueryStatusReportListeners queryTrackers) {
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
        String destination = resultsPublisherConfig.get(ResultsOutput.DESTINATION);
        if (SQSResultsOutput.SQS.equals(destination)) {
            return new SQSResultsOutput(instanceProperties, sqsClient, tableProperties.getSchema(), resultsPublisherConfig);
        } else if (S3ResultsOutput.S3.equals(destination)) {
            return new S3ResultsOutput(instanceProperties, tableProperties, resultsPublisherConfig);
        } else if (WebSocketOutput.DESTINATION_NAME.equals(destination)) {
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
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;
        private LeafPartitionRowRetrieverProvider rowRetrieverProvider;
        private SqsClient sqsClient;
        private S3Client s3Client;
        private DynamoDbClient dynamoClient;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder rowRetrieverProvider(LeafPartitionRowRetrieverProvider rowRetrieverProvider) {
            this.rowRetrieverProvider = rowRetrieverProvider;
            return this;
        }

        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        public SqsLeafPartitionQueryProcessor build() throws ObjectFactoryException {
            return new SqsLeafPartitionQueryProcessor(this);
        }
    }
}
