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
package sleeper.clients.api;

/**
 * Thrown when the results of a query are not found in the S3 query results bucket. This can happen if the query was
 * processed with a different output destination, or if the query results have been deleted or expired. A query that
 * returns no results outputs an empty file, so this exception should not result from a query which returns no results.
 */
public class QueryResultsNotFoundException extends RuntimeException {

    public QueryResultsNotFoundException(String queryId, Throwable cause) {
        super("No results found for query " + queryId + ". " +
                "Either the query was never processed with this results output, or its results have been deleted.",
                cause);
    }
}
