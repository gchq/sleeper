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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.clients.query.exception.WebSocketTimeoutException;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An iterator that receives results of a query from a web socket and returns them to the user.
 */
public class QueryWebSocketIterator implements CloseableIterator<Row>, QueryWebSocketHandler {

    private final long timeoutMilliseconds;
    private final Runnable closeWebSocket;
    private final CompletableFuture<List<Row>> future = new CompletableFuture<>();
    private List<Row> rows;
    private int index = 0;

    public QueryWebSocketIterator(long timeoutMilliseconds, Runnable closeWebSocket) {
        this.timeoutMilliseconds = timeoutMilliseconds;
        this.closeWebSocket = closeWebSocket;
    }

    @Override
    public boolean hasNext() {
        return index <= (getRowList().size() - 1);
    }

    @Override
    public Row next() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Row row = getRowList().get(index);
        index++;
        return row;
    }

    @Override
    public void close() {
        closeWebSocket.run();
    }

    @Override
    public void handleException(RuntimeException e) {
        future.completeExceptionally(e);
    }

    @Override
    public void handleResults(List<Row> results) {
        future.complete(results);
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    private List<Row> getRowList() {
        if (rows == null) {
            try {
                if (timeoutMilliseconds >= 0) {
                    rows = future.get(timeoutMilliseconds, TimeUnit.MILLISECONDS);
                } else {
                    rows = future.get();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            } catch (ExecutionException ee) {
                throw (RuntimeException) ee.getCause();
            } catch (TimeoutException e) {
                throw new WebSocketTimeoutException(timeoutMilliseconds, e);
            }
        }
        return rows;
    }
}
