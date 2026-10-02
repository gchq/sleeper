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
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputProvider;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.runner.tracker.QueryStatusReportListeners;

import java.io.IOException;

public class SqsLeafPartitionQueryProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsLeafPartitionQueryProcessor.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final LeafPartitionRowRetrieverProvider rowRetrieverProvider;
    private final ResultsOutputProvider resultsOutputProvider;
    private final ObjectFactory objectFactory;
    private final QueryStatusReportListener queryTracker;

    public SqsLeafPartitionQueryProcessor(
            TablePropertiesProvider tablePropertiesProvider, LeafPartitionRowRetrieverProvider rowRetrieverProvider,
            ResultsOutputProvider resultsOutputProvider, ObjectFactory objectFactory, QueryStatusReportListener queryTracker) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.rowRetrieverProvider = rowRetrieverProvider;
        this.resultsOutputProvider = resultsOutputProvider;
        this.objectFactory = objectFactory;
        this.queryTracker = queryTracker;
    }

    public void processQuery(LeafPartitionQuery leafPartitionQuery) {
        QueryOrLeafPartitionQuery query = new QueryOrLeafPartitionQuery(leafPartitionQuery);
        QueryStatusReportListeners queryTrackers = QueryStatusReportListeners.fromConfig(
                leafPartitionQuery.getProcessingConfig().getStatusReportDestinations());
        queryTrackers.add(queryTracker);

        ResultsOutputInfo outputInfo = null;
        try {
            TableProperties tableProperties = query.getTableProperties(tablePropertiesProvider);
            queryTrackers.queryInProgress(leafPartitionQuery);
            try (CloseableIterator<Row> results = getLeafPartitionQueryExecutor(tableProperties).getRows(leafPartitionQuery)) {
                outputInfo = resultsOutputProvider.getResultsOutput(tableProperties, leafPartitionQuery).publish(query, results);
            }
            query.reportCompleted(queryTrackers, outputInfo);
        } catch (IOException | QueryException | RuntimeException e) {
            LOGGER.error("Exception thrown executing subquery {} under query {}", leafPartitionQuery.getSubQueryId(), leafPartitionQuery.getQueryId(), e);
            if (outputInfo != null) {
                query.reportCompleted(queryTrackers, outputInfo.withError(e));
            } else {
                query.reportFailed(queryTrackers, e);
            }
        }
    }

    private LeafPartitionQueryExecutor getLeafPartitionQueryExecutor(TableProperties tableProperties) {
        return new LeafPartitionQueryExecutor(
                objectFactory, tableProperties,
                rowRetrieverProvider.getRowRetriever(tableProperties));
    }
}
