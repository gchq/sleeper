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
package sleeper.clients.query;

import sleeper.core.row.Row;

import java.util.List;

/**
 * Handles events from a WebSocket query session.
 */
public interface QueryWebSocketHandler {

    /**
     * Handles an exception that occurred during query processing.
     *
     * @param e the exception
     */
    void handleException(RuntimeException e);

    /**
     * Handles the final results of a query.
     *
     * @param results the rows returned by the query
     */
    void handleResults(List<Row> results);
}
