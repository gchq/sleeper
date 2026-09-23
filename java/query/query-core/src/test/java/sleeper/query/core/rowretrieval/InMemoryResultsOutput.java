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
package sleeper.query.core.rowretrieval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.core.output.ResultsOutputProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A results output to hold query results in memory.
 */
public class InMemoryResultsOutput implements ResultsOutput, ResultsOutputProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryResultsOutput.class);

    private final Map<String, List<Row>> queryIdToPublishedResults = new HashMap<>();

    @Override
    public ResultsOutputInfo publish(QueryOrLeafPartitionQuery query, CloseableIterator<Row> results) {
        List<Row> target = queryIdToPublishedResults.computeIfAbsent(query.getQueryId(), id -> new ArrayList<>());
        long rows = 0;
        while (results.hasNext()) {
            target.add(results.next());
            rows++;
        }
        return new ResultsOutputInfo(rows, List.of(new ResultsOutputLocation("in-memory", "in-memory")));
    }

    @Override
    public ResultsOutput getResultsOutput(TableProperties tableProperties, LeafPartitionQuery query) {
        Map<String, String> resultsPublisherConfig = query.getProcessingConfig().getResultsPublisherConfig();
        if (resultsPublisherConfig != null && resultsPublisherConfig.containsKey(DESTINATION)) {
            LOGGER.error("Unknown results publisher config: {}", resultsPublisherConfig);
            throw new RuntimeException("Unknown results publisher for destination: " + resultsPublisherConfig.get(DESTINATION));
        }
        return this;
    }

    public Map<String, List<Row>> getQueryIdToPublishedResults() {
        return queryIdToPublishedResults;
    }

    /**
     * Streams through the rows published for all queries.
     *
     * @return the rows
     */
    public Stream<Row> streamPublishedResults() {
        return queryIdToPublishedResults.values().stream().flatMap(List::stream);
    }

}
