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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.query.QueryTypeDsl;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.query.QueryRange.range;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

/**
 * Web socket queries are covered separately here. They need much more system test coverage than other query types since
 * we can't test them fully without deploying an Amazon API Gateway.
 */
@SystemTest
public class QueryWebSocketST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setup(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldRunQueryForAllRows(QueryTypeDsl queryType, SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(queryType.query(sleeper)
                .allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 100));
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldRunQueryWithOneRange(QueryTypeDsl queryType, SleeperDsl sleeper) {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(queryType.query(sleeper)
                .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        range("row-10", "row-20")))
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(10, 20));
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldRunQueryWithTwoRangesThatOverlap(QueryTypeDsl queryType, SleeperDsl sleeper) {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(queryType.query(sleeper)
                .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        range("row-10", "row-30"),
                        range("row-20", "row-40")))
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(10, 40));
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldRunQueryWithTwoRangesThatDoNotOverlap(QueryTypeDsl queryType, SleeperDsl sleeper) {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(queryType.query(sleeper)
                .byRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        range("row-10", "row-20"),
                        range("row-30", "row-40")))
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableFrom(
                        () -> LongStream.concat(
                                LongStream.range(10, 20),
                                LongStream.range(30, 40))));
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldRunQueryReturningNoRows(QueryTypeDsl queryType, SleeperDsl sleeper) {
        // When/Then
        assertThat(queryType.query(sleeper)
                .allRowsInTable())
                .isEmpty();
    }

    // We established an appropriate number of rows for this by roughly checking the payload size produced in
    // QueryWebSocketMessageSerDeTest and comparing against the AWS docs:
    // https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-execution-service-websocket-limits-table.html
    @Test
    void shouldRunQueryReturningTooManyRowsForOneWebSocketMessage(QueryTypeDsl queryType, SleeperDsl sleeper) {
        // Given we have a number of rows that will break API Gateway's limit on payload size
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 3000));

        // When/Then
        assertThat(queryType.query(sleeper).allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 3000));
    }

    private static Stream<Arguments> queryTypes() {
        return Stream.of(
                Arguments.of(Named.of("Java data engine",
                        QueryTypeDsl.builder().java().webSocket().build())),
                Arguments.of(Named.of("DataFusion data engine",
                        QueryTypeDsl.builder().dataFusion().webSocket().build())));
    }
}
