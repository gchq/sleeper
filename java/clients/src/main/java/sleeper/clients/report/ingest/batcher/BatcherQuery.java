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

package sleeper.clients.report.ingest.batcher;

import sleeper.clients.report.ingest.batcher.query.AllFilesQuery;
import sleeper.clients.report.ingest.batcher.query.BatcherQueryPrompt;
import sleeper.clients.report.ingest.batcher.query.PendingFilesQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.util.List;

/**
 * A query to generate a report based on files in the ingest batcher store. Different types of query can include files
 * based on their status or other parameters.
 */
public interface BatcherQuery {

    /**
     * Retrieves file tracking information from the store that matches this query.
     *
     * @param  store the ingest batcher store
     * @return       the file tracking information
     */
    List<IngestBatcherTrackedFile> run(IngestBatcherStore store);

    /**
     * Retrieves the type of this query.
     *
     * @return the type
     */
    Type getType();

    /**
     * Creates a query from a query type, prompting the user for more information if necessary.
     *
     * @param  queryType the query type
     * @param  in        the console to prompt the user for more information
     * @return           the query
     */
    static BatcherQuery from(BatcherQuery.Type queryType, ConsoleInput in) {
        if (queryType == Type.PROMPT) {
            return BatcherQueryPrompt.from(in);
        } else if (queryType == Type.ALL) {
            return new AllFilesQuery();
        } else if (queryType == Type.PENDING) {
            return new PendingFilesQuery();
        } else {
            throw new IllegalArgumentException("Unexpected query type: " + queryType);
        }

    }

    /**
     * The type of a query for a report on files tracked in the ingest batcher store.
     */
    enum Type {
        PROMPT,
        ALL,
        PENDING
    }
}
