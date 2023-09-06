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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@Tag("SystemTest")
public class QueryIT {
    @TempDir
    private Path tempDir;
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setup() {
        sleeper.connectToInstance(MAIN);
        sleeper.queryResults().emptyBucket();
    }

    @Nested
    @DisplayName("Direct query")
    class DirectQuery {
        @Test
        void shouldRunQueryForAllRecords() {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        }

        @Test
        void shouldRunQueryWithOneRange() {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .run("key", "row-0000000000000000010", "row-0000000000000000020"))
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(10, 20)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatOverlap() {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .run("key",
                            "row-0000000000000000010", "row-0000000000000000030",
                            "row-0000000000000000020", "row-0000000000000000040"))
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(10, 40)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatDoNotOverlap() {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .run("key",
                            "row-0000000000000000010", "row-0000000000000000020",
                            "row-0000000000000000030", "row-0000000000000000040"))
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.concat(
                            LongStream.range(10, 20), LongStream.range(30, 40))));
        }
    }

    @Nested
    @DisplayName("SQS Query")
    class SQSQuery {
        @Test
        void shouldRunQueryForAllRecords() throws InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .allRecordsInTable()
                    .waitForQuery().results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        }

        @Test
        void shouldRunQueryWithOneRange() throws InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .run("key", "row-0000000000000000010", "row-0000000000000000020")
                    .waitForQuery().results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(10, 20)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatOverlap() throws InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .run("key",
                            "row-0000000000000000010", "row-0000000000000000030",
                            "row-0000000000000000020", "row-0000000000000000040")
                    .waitForQuery().results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(10, 40)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatDoNotOverlap() throws InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .run("key",
                            "row-0000000000000000010", "row-0000000000000000020",
                            "row-0000000000000000030", "row-0000000000000000040")
                    .waitForQuery().results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.concat(
                            LongStream.range(10, 20), LongStream.range(30, 40))));
        }
    }
}
