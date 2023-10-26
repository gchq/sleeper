/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.query.model;

public class QueryValidationException extends RuntimeException {
    private final String queryId;

    public QueryValidationException(String queryId, String message) {
        super("Query validation failed for query \"" + queryId + "\": " + message);
        this.queryId = queryId;
    }

    public QueryValidationException(String queryId, Exception cause) {
        super("Query validation failed for query \"" + queryId + "\": " + cause.getMessage(), cause);
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }
}
