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
package sleeper.systemtest.dsl.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.util.SystemTestSchema;

import java.nio.file.Path;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.query.QueryRange.range;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class QueryTest {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setup(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldRunQuery(QueryTypeDsl queryType, SleeperDsl sleeper) {
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

    private static Stream<Arguments> queryTypes() {
        return Stream.of(
                Arguments.of(Named.of("Java data engine, direct",
                        QueryTypeDsl.builder().java().direct().build())),
                Arguments.of(Named.of("Java data engine, by SQS queue",
                        QueryTypeDsl.builder().java().byQueue().build())),
                Arguments.of(Named.of("DataFusion data engine, direct",
                        QueryTypeDsl.builder().dataFusion().direct().build())),
                Arguments.of(Named.of("DataFusion data engine, by SQS queue",
                        QueryTypeDsl.builder().dataFusion().byQueue().build())));
    }

}
