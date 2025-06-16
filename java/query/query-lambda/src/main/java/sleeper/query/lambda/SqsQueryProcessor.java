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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
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
import sleeper.query.core.recordretrieval.QueryExecutor;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.query.runner.tracker.QueryStatusReportListeners;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class SqsQueryProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueryProcessor.class);

    private final ExecutorService executorService;
    private final InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final ObjectFactory objectFactory;
    private final QueryStatusReportListener queryListener;
    private final Map<String, QueryExecutor> queryExecutorCache = new HashMap<>();

    private SqsQueryProcessor(Builder builder) throws ObjectFactoryException {
        sqsClient = builder.sqsClient;
        instanceProperties = builder.instanceProperties;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        stateStoreProvider = builder.stateStoreProvider;
        executorService = builder.executorService;
        objectFactory = builder.objectFactory;
        queryListener = builder.queryListener;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void processQuery(QueryOrLeafPartitionQuery query) {
        QueryStatusReportListeners queryListeners = QueryStatusReportListeners.fromConfig(
                query.getProcessingConfig().getStatusReportDestinations());
        queryListeners.add(queryListener);
        try {
            TableProperties tableProperties = query.getTableProperties(tablePropertiesProvider);
            Query parentQuery = query.asParentQuery();
            queryListeners.queryInProgress(parentQuery);
            processRangeQuery(parentQuery, tableProperties, queryListeners);
        } catch (RuntimeException | QueryException e) {
            LOGGER.error("Exception thrown executing query", e);
            query.reportFailed(queryListeners, e);
        }
    }

    private void processRangeQuery(Query query, TableProperties tableProperties, QueryStatusReportListeners queryTrackers) throws QueryException {
        QueryExecutor queryExecutor = queryExecutorCache.computeIfAbsent(tableProperties.get(TABLE_ID), tableID -> {
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            Configuration conf = HadoopConfigurationProvider.getConfigurationForQueryLambdas(instanceProperties, tableProperties);
            return new QueryExecutor(objectFactory, tableProperties, stateStore,
                    new LeafPartitionRecordRetrieverImpl(executorService, conf, tableProperties));
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
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(sqsLeafPartitionQueryQueueURL)
                    .messageBody(serialisedQuery)
                    .build());
        }
        queryTrackers.subQueriesCreated(query, subQueries);
        LOGGER.info("Submitted {} subqueries to queue", subQueries.size());
    }

    public static final class Builder {
        private SqsClient sqsClient;
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;
        private StateStoreProvider stateStoreProvider;
        private ObjectFactory objectFactory;
        private ExecutorService executorService;
        private QueryStatusReportListener queryListener;

        private Builder() {
        }

        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
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

        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder executorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public Builder queryListener(QueryStatusReportListener queryListener) {
            this.queryListener = queryListener;
            return this;
        }

        public SqsQueryProcessor build() throws ObjectFactoryException {
            return new SqsQueryProcessor(this);
        }
    }
}
