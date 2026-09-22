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
package sleeper.query.runner.output;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutputInfo;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class NoResultsOutputTest {

    private final QueryOrLeafPartitionQuery query = new QueryOrLeafPartitionQuery(Query.builder()
            .tableName("test-table")
            .queryId("test-query")
            .regions(List.of())
            .build());

    @Test
    void shouldCloseResultsWithoutPublishingThem() {
        // Given
        CloseTrackingIterator results = new CloseTrackingIterator(null);

        // When
        ResultsOutputInfo outputInfo = new NoResultsOutput().publish(query, results);

        // Then
        assertThat(results.closed).isTrue();
        assertThat(outputInfo.getRowCount()).isZero();
        assertThat(outputInfo.getError()).isNull();
    }

    @Test
    void shouldReportErrorWhenResultsFailToClose() {
        // Given
        IOException failure = new IOException("Failed to close");
        CloseTrackingIterator results = new CloseTrackingIterator(failure);

        // When
        ResultsOutputInfo outputInfo = new NoResultsOutput().publish(query, results);

        // Then
        assertThat(outputInfo.getRowCount()).isZero();
        assertThat(outputInfo.getError()).isSameAs(failure);
    }

    private static class CloseTrackingIterator implements CloseableIterator<Row> {
        private final IOException failOnClose;
        private boolean closed = false;

        CloseTrackingIterator(IOException failOnClose) {
            this.failOnClose = failOnClose;
        }

        @Override
        public boolean hasNext() {
            throw new IllegalStateException("Results should not be read");
        }

        @Override
        public Row next() {
            throw new IllegalStateException("Results should not be read");
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (failOnClose != null) {
                throw failOnClose;
            }
        }
    }
}
