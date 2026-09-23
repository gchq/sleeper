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
package sleeper.query.lambda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputProvider;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.runner.tracker.QueryStatusReportListeners;

public class LeafPartitionQueryProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeafPartitionQueryProcessor.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final LeafPartitionRowRetrieverProvider rowRetrieverProvider;
    private final ResultsOutputProvider resultsOutputProvider;
    private final ObjectFactory objectFactory;
    private final QueryStatusReportListener queryTracker;

    private LeafPartitionQueryProcessor(Builder builder) {
        tablePropertiesProvider = builder.tablePropertiesProvider;
        rowRetrieverProvider = builder.rowRetrieverProvider;
        resultsOutputProvider = builder.resultsOutputProvider;
        objectFactory = builder.objectFactory;
        queryTracker = builder.queryTracker;
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
            try (CloseableIterator<Row> results = getLeafPartitionQueryExecutor(tableProperties).getRows(leafPartitionQuery)) {
                ResultsOutputInfo outputInfo = resultsOutputProvider.getResultsOutput(tableProperties, leafPartitionQuery).publish(query, results);

                query.reportCompleted(queryTrackers, outputInfo);
            }
        } catch (Exception e) {
            LOGGER.error("Exception thrown executing subquery {} under query {}", leafPartitionQuery.getSubQueryId(), leafPartitionQuery.getQueryId(), e);
            query.reportFailed(queryTrackers, e);
        }
    }

    private LeafPartitionQueryExecutor getLeafPartitionQueryExecutor(TableProperties tableProperties) {
        return new LeafPartitionQueryExecutor(
                objectFactory, tableProperties,
                rowRetrieverProvider.getRowRetriever(tableProperties));
    }

    public static final class Builder {
        private TablePropertiesProvider tablePropertiesProvider;
        private LeafPartitionRowRetrieverProvider rowRetrieverProvider;
        private ResultsOutputProvider resultsOutputProvider;
        private ObjectFactory objectFactory;
        private QueryStatusReportListener queryTracker;

        private Builder() {
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder rowRetrieverProvider(LeafPartitionRowRetrieverProvider rowRetrieverProvider) {
            this.rowRetrieverProvider = rowRetrieverProvider;
            return this;
        }

        public Builder resultsOutputProvider(ResultsOutputProvider resultsOutputProvider) {
            this.resultsOutputProvider = resultsOutputProvider;
            return this;
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder queryTracker(QueryStatusReportListener queryTracker) {
            this.queryTracker = queryTracker;
            return this;
        }

        public LeafPartitionQueryProcessor build() {
            return new LeafPartitionQueryProcessor(this);
        }
    }
}
