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
package sleeper.clients.query;

import sleeper.core.row.Row;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class QueryWebSocketFuture<T> extends CompletableFuture<List<Row>> implements QueryWebSocketHandler {

    @Override
    public void handleException(RuntimeException e) {
        this.completeExceptionally(e);
    }

    @Override
    public void handleResults(List<Row> results) {
        this.complete(results);
    }

}
