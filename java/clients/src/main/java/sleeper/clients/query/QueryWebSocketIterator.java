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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * An iterator that receives results of a query from a web socket and returns them to the user.
 */
public class QueryWebSocketIterator implements CloseableIterator<Row>, QueryWebSocketHandler {

    private CompletableFuture<List<Row>> future = new CompletableFuture<>();

    @Override
    public boolean hasNext() {
        return !getRowList().isEmpty();
    }

    @Override
    public Row next() {
        return getRowList().remove(0);
    }

    @Override
    public void close() {
    }

    @Override
    public void handleException(RuntimeException e) {
        future.completeExceptionally(e);
    }

    @Override
    public void handleResults(List<Row> results) {
        List<Row> rows = new ArrayList<>(results);
        future.complete(rows);
    }

    private List<Row> getRowList() {
        try {
            return future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        } catch (ExecutionException ee) {
            throw (RuntimeException) ee.getCause();
        }

    }
}
