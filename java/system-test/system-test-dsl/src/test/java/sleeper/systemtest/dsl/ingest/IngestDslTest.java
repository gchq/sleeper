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

package sleeper.systemtest.dsl.ingest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class IngestDslTest {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldIngestByQueue(SleeperDsl sleeper) {
        // Given
        Row row = new Row(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));
        sleeper.sourceFiles().create("test.parquet", row);

        // When
        sleeper.ingest().byQueue()
                .sendSourceFiles("test.parquet")
                .waitForTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactly(row);
        assertThat(sleeper.tableFiles().references())
                .hasSize(1);
    }

    @Test
    void shouldBulkImportByQueue(SleeperDsl sleeper) {
        // Given
        Row row = new Row(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));
        sleeper.sourceFiles().create("test.parquet", row);

        // When
        sleeper.ingest().bulkImportByQueue()
                .sendSourceFiles("test.parquet").waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactly(row);
        assertThat(sleeper.tableFiles().references())
                .hasSize(1);
    }

    @Test
    void shouldIngestSplitIntoFiles(SleeperDsl sleeper) {
        // Given
        RowNumbers numbers = sleeper.scrambleNumberedRows(LongStream.range(0, 100_000));

        // When
        sleeper.ingest().direct(null)
                .splitIngests(1_000, numbers);

        // Then
        assertThat(new HashSet<>(sleeper.directQuery().allRowsInTable()))
                .isEqualTo(setFrom(sleeper.generateNumberedRows()
                        .iterableOverRange(0, 100_000)));
        assertThat(sleeper.tableFiles().references())
                .hasSize(1_000);
    }

    @Test
    void shouldNotSplitIntoFilesIfNotExactSplit(SleeperDsl sleeper) {
        // Given
        RowNumbers numbers = sleeper.scrambleNumberedRows(LongStream.range(0, 10));
        DirectIngestDsl ingest = sleeper.ingest().direct(null);

        // When / Then
        assertThatThrownBy(() -> ingest.splitIngests(3, numbers))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static <T> Set<T> setFrom(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(toSet());
    }
}
