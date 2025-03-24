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

import sleeper.core.record.Record;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class SystemTestIngestTest {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldIngestByQueue(SleeperSystemTest sleeper) {
        // Given
        Record record = new Record(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));
        sleeper.sourceFiles().create("test.parquet", record);

        // When
        sleeper.ingest().byQueue()
                .sendSourceFiles("test.parquet")
                .waitForTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
        assertThat(sleeper.tableFiles().references())
                .hasSize(1);
    }

    @Test
    void shouldBulkImportByQueue(SleeperSystemTest sleeper) {
        // Given
        Record record = new Record(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));
        sleeper.sourceFiles().create("test.parquet", record);

        // When
        sleeper.ingest().bulkImportByQueue()
                .sendSourceFiles("test.parquet").waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
        assertThat(sleeper.tableFiles().references())
                .hasSize(1);
    }

    @Test
    void shouldIngestSplitIntoFiles(SleeperSystemTest sleeper) {
        // Given
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 100_000));

        // When
        sleeper.ingest().direct(null)
                .splitIntoFiles(1_000, numbers);

        // Then
        assertThat(new HashSet<>(sleeper.directQuery().allRecordsInTable()))
                .isEqualTo(setFrom(sleeper.generateNumberedRecords(LongStream.range(0, 100_000))));
        assertThat(sleeper.tableFiles().references())
                .hasSize(1_000);
    }

    private static <T> Set<T> setFrom(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(toSet());
    }
}
