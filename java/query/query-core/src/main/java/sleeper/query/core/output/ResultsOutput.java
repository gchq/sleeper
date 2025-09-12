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
package sleeper.query.core.output;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;

/**
 * Results output. Implemented by classes that need to send results of queries somewhere.
 */
public interface ResultsOutput {

    String DESTINATION = "destination";

    /**
     * Publishes the results obtained from a given query or a specific leaf partition query.
     *
     * @param  query   the query definition
     * @param  results the query results to publish
     * @return         the results
     */
    ResultsOutputInfo publish(QueryOrLeafPartitionQuery query, CloseableIterator<Row> results);
}
