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

import org.junit.jupiter.api.Test;

import sleeper.clients.api.QueryResultsReader.ResultsFile;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.WrappedIterator;
import sleeper.core.row.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ParallelResultsFileIteratorTest {

    @Test
    void shouldReadRowsFromMultipleFiles() throws Exception {
        // Given
        List<ResultsFile> files = List.of(
                file("file-1.parquet", rows("A", "B")),
                file("file-2.parquet", rows("C")),
                file("file-3.parquet", rows("D", "E")));

        // When
        List<Row> found = readAll(files, 2);

        // Then
        assertThat(found).containsExactlyInAnyOrderElementsOf(rows("A", "B", "C", "D", "E"));
    }

    @Test
    void shouldReadMoreFilesThanThreads() throws Exception {
        // Given
        List<ResultsFile> files = List.of(
                file("file-1.parquet", rows("A")),
                file("file-2.parquet", rows("B")),
                file("file-3.parquet", rows("C")),
                file("file-4.parquet", rows("D")));

        // When
        List<Row> found = readAll(files, 1);

        // Then
        assertThat(found).containsExactlyInAnyOrderElementsOf(rows("A", "B", "C", "D"));
    }

    @Test
    void shouldReadNoFiles() throws Exception {
        // When
        List<Row> found = readAll(List.of(), 2);

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    void shouldReadEmptyFile() throws Exception {
        // Given
        List<ResultsFile> files = List.of(file("file-1.parquet", List.of()));

        // When
        List<Row> found = readAll(files, 2);

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    void shouldReturnRowsFromFasterFileWhileSlowerFileIsBlocked() throws Exception {
        // Given a file that blocks until released, and a file whose rows are available immediately
        CountDownLatch slowFileReleased = new CountDownLatch(1);
        List<ResultsFile> files = List.of(
                new ResultsFile("slow.parquet", () -> {
                    awaitOrFail(slowFileReleased);
                    return new WrappedIterator<>(rows("Z").iterator());
                }),
                file("fast.parquet", rows("A", "B")));

        // When / Then the fast file's rows are returned while the slow file is still blocked
        List<Row> found = new ArrayList<>();
        try (CloseableIterator<Row> iterator = new ParallelResultsFileIterator(files, 2)) {
            found.add(iterator.next());
            found.add(iterator.next());
            assertThat(found).containsExactlyInAnyOrderElementsOf(rows("A", "B"));

            // And the slow file's rows are returned once it is released
            slowFileReleased.countDown();
            iterator.forEachRemaining(found::add);
        }
        assertThat(found).containsExactlyInAnyOrderElementsOf(rows("A", "B", "Z"));
    }

    @Test
    void shouldFailWhenReadingAFileFails() throws Exception {
        // Given
        RuntimeException failure = new RuntimeException("Could not read file");
        List<ResultsFile> files = List.of(
                new ResultsFile("bad.parquet", () -> {
                    throw failure;
                }));

        // When / Then
        try (CloseableIterator<Row> iterator = new ParallelResultsFileIterator(files, 2)) {
            assertThatThrownBy(iterator::hasNext).isSameAs(failure);
        }
    }

    @Test
    void shouldReadRowsFromOtherFilesWhenOneFileFails() throws Exception {
        // Given a file that fails after the other file has been fully read
        CountDownLatch goodFileRead = new CountDownLatch(1);
        RuntimeException failure = new RuntimeException("Could not read file");
        List<ResultsFile> files = List.of(
                new ResultsFile("bad.parquet", () -> {
                    awaitOrFail(goodFileRead);
                    throw failure;
                }),
                file("good.parquet", rows("A")));

        // When / Then the good file's rows are returned, then the failure surfaces
        List<Row> found = new ArrayList<>();
        try (CloseableIterator<Row> iterator = new ParallelResultsFileIterator(files, 2)) {
            found.add(iterator.next());
            goodFileRead.countDown();
            assertThatThrownBy(iterator::hasNext).isSameAs(failure);
        }
        assertThat(found).containsExactlyElementsOf(rows("A"));
    }

    @Test
    void shouldStopReadingWhenClosedBeforeAllRowsWereRead() throws Exception {
        // Given more rows than fit in the queue would block the reader, and a consumer that stops early
        List<Row> manyRows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            manyRows.add(new Row(Map.of("key", "row-" + i)));
        }
        List<ResultsFile> files = List.of(file("file-1.parquet", manyRows));

        // When only one row is read before closing
        List<Row> found = new ArrayList<>();
        try (CloseableIterator<Row> iterator = new ParallelResultsFileIterator(files, 2)) {
            found.add(iterator.next());
        }

        // Then
        assertThat(found).hasSize(1);
    }

    @Test
    void shouldRefuseFewerThanOneThread() {
        assertThatThrownBy(() -> new ParallelResultsFileIterator(List.of(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private List<Row> readAll(List<ResultsFile> files, int threads) throws Exception {
        List<Row> found = new ArrayList<>();
        try (CloseableIterator<Row> iterator = new ParallelResultsFileIterator(files, threads)) {
            iterator.forEachRemaining(found::add);
        }
        return found;
    }

    private ResultsFile file(String name, List<Row> rows) {
        return new ResultsFile(name, () -> new WrappedIterator<>(rows.iterator()));
    }

    private List<Row> rows(String... keys) {
        List<Row> rows = new ArrayList<>();
        for (String key : keys) {
            rows.add(new Row(Map.of("key", key)));
        }
        return rows;
    }

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
