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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.query.QueryRange.range;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class QuerySqsST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setup(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @AfterEach
    void tearDown(SleeperDsl sleeper) {
        sleeper.query().emptyResultsBucket();
    }

    @Test
    void shouldRunQueryForAllRows(SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(sleeper.query().byQueue()
                .allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 100));
    }

    @Test
    void shouldRunQueryWithOneRange(SleeperDsl sleeper) {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(sleeper.query().byQueue()
                .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        range("row-10", "row-20")))
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(10, 20));
    }

    @Test
    void shouldRunQueryWithTwoRangesThatOverlap(SleeperDsl sleeper) {
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
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(10, 40));
    }

    @Test
    void shouldRunQueryWithTwoRangesThatDoNotOverlap(SleeperDsl sleeper) {
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
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableFrom(
                        () -> LongStream.concat(
                                LongStream.range(10, 20),
                                LongStream.range(30, 40))));
    }
}
