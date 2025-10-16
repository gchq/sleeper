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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.Test2;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.query.QueryRange.range;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MULTI_TABLES;

@SystemTest
@Test2
public class QueryST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setup(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MULTI_TABLES);
    }

    @Nested
    @DisplayName("Direct query")
    class DirectQuery {
        @Test
        void shouldRunQueryForAllRows(SleeperSystemTest sleeper) {
            // Given
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .allRowsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(0, 100)));
        }

        @Test
        void shouldRunQueryWithOneRange(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-20")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(10, 20)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatOverlap(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-30"),
                            range("row-20", "row-40")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(10, 40)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatDoNotOverlap(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().direct()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-20"),
                            range("row-30", "row-40")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.concat(
                            LongStream.range(10, 20), LongStream.range(30, 40))));
        }
    }

    @Nested
    @DisplayName("SQS Query")
    class SQSQuery {
        @AfterEach
        void tearDown(SleeperSystemTest sleeper) {
            sleeper.query().emptyResultsBucket();
        }

        @Test
        void shouldRunQueryForAllRows(SleeperSystemTest sleeper) {
            // Given
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .allRowsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(0, 100)));
        }

        @Test
        void shouldRunQueryWithOneRange(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-20")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(10, 20)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatOverlap(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-30"),
                            range("row-20", "row-40")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(10, 40)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatDoNotOverlap(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().byQueue()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-20"),
                            range("row-30", "row-40")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.concat(
                            LongStream.range(10, 20), LongStream.range(30, 40))));
        }
    }

    @Nested
    @DisplayName("WebSocket query")
    class WebSocketQuery {
        @Test
        void shouldRunQueryForAllRows(SleeperSystemTest sleeper) {
            // Given
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().webSocket()
                    .allRowsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(0, 100)));
        }

        @Test
        void shouldRunQueryWithOneRange(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().webSocket()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-20")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(10, 20)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatOverlap(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().webSocket()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-30"),
                            range("row-20", "row-40")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(10, 40)));
        }

        @Test
        void shouldRunQueryWithTwoRangesThatDoNotOverlap(SleeperSystemTest sleeper) {
            // Given
            sleeper.setGeneratorOverrides(
                    overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.query().webSocket()
                    .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                            range("row-10", "row-20"),
                            range("row-30", "row-40")))
                    .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.concat(
                            LongStream.range(10, 20), LongStream.range(30, 40))));
        }

        @Test
        void shouldRunQueryReturningNoRows(SleeperSystemTest sleeper) {
            // When/Then
            assertThat(sleeper.query().webSocket()
                    .allRowsInTable())
                    .isEmpty();
        }
    }
}
