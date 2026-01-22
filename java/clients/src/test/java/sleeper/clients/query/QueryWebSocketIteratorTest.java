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

import org.junit.jupiter.api.Test;

import sleeper.clients.query.exception.WebSocketTimeoutException;
import sleeper.core.row.Row;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class QueryWebSocketIteratorTest {

    // When I iterate through the data and a successful result occurs, the rows are returned
    // When I iterate through the data and a failure occurs, the exception is thrown out of the iterator
    // When I iterate through the data, it will block until results are available
    // When I iterate through the data without any results or failure, it times out with a configurable wait time (set to zero or close to zero for the test)
    // When I close the iterator the web socket should close

    int numTimesWebSocketClosed = 0;
    long timeoutMilliseconds = -1;

    @Test
    void shouldReturnRows() {
        // Given
        List<Row> rows = List.of(new Row(Map.of("key", "value")));
        try (QueryWebSocketIterator iterator = iterator()) {
            iterator.handleResults(rows);

            // When / Then
            assertThat(iterator).toIterable()
                    .containsExactlyElementsOf(rows);
        }
    }

    @Test
    void shouldThrowExceptionWhenFailureOccurs() {
        // Given
        RuntimeException exception = new RuntimeException();
        try (QueryWebSocketIterator iterator = iterator()) {
            iterator.handleException(exception);

            // When / Then
            assertThatThrownBy(() -> iterator.hasNext())
                    .isSameAs(exception);
            assertThatThrownBy(() -> iterator.next())
                    .isSameAs(exception);
        }
    }

    @Test
    void shouldReturnRowsAsync() throws Exception {
        // Given
        Row row = new Row(Map.of("key", "value"));
        try (QueryWebSocketIterator iterator = iterator()) {
            ForkJoinTask<Row> task = ForkJoinPool.commonPool().submit(iterator::next);
            Thread.sleep(1);
            iterator.handleResults(List.of(row));

            // When / Then
            assertThat(task.join()).isEqualTo(row);
        }
    }

    @Test
    void shouldTimeOutWaitingForResponseWhenConfigured() {
        // Given
        setTimeout(1);
        try (QueryWebSocketIterator iterator = iterator()) {
            // When / Then
            assertThatThrownBy(() -> iterator.next())
                    .isInstanceOf(WebSocketTimeoutException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void shouldGetResultsWhenTimeoutIsConfigured() {
        // Given
        setTimeout(1);
        List<Row> rows = List.of(new Row(Map.of("key", "value")));
        try (QueryWebSocketIterator iterator = iterator()) {
            iterator.handleResults(rows);

            // When / Then
            assertThat(iterator).toIterable()
                    .containsExactlyElementsOf(rows);
        }
    }

    @Test
    void shouldCloseWebSocket() {
        // When an iterator is opened and closed
        try (QueryWebSocketIterator iterator = iterator()) {
        }
        // Then the web socket is closed
        assertThat(numTimesWebSocketClosed).isEqualTo(1);
    }

    private void setTimeout(long milliseconds) {
        timeoutMilliseconds = milliseconds;
    }

    private QueryWebSocketIterator iterator() {
        return new QueryWebSocketIterator(timeoutMilliseconds, () -> numTimesWebSocketClosed++);
    }

}
