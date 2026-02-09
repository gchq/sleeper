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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class UserJarsST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceNoTables(MAIN);
        sleeper.tables().createWithProperties("test",
                Schema.builder()
                        .rowKeyFields(new Field("key", new StringType()))
                        .valueFields(new Field("timestamp", new LongType()))
                        .build(),
                Map.of(TABLE_ONLINE, "false"));
        sleeper.setGeneratorOverrides(
                overrideField("key",
                        numberStringAndZeroPadTo(3).then(addPrefix("row-"))));
    }

    @Test
    void shouldApplyTableIteratorFromUserJarDuringIngest(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.sourceFiles().createWithNumberedRows("test.parquet", LongStream.range(0, 100));
        sleeper.updateTableProperties(Map.of(
                ITERATOR_CLASS_NAME, "sleeper.example.iterator.FixedAgeOffIterator",
                ITERATOR_CONFIG, "timestamp,50"));

        // When
        sleeper.ingest().byQueue().sendSourceFiles("test.parquet").waitForTask().waitForJobs();

        // Then
        assertThat(sleeper.query().byQueue().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows().iterableOverRange(50, 100));
    }

    @Test
    void shouldApplyTableIteratorFromUserJarDuringCompaction(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));
        sleeper.updateTableProperties(Map.of(
                ITERATOR_CLASS_NAME, "sleeper.example.iterator.FixedAgeOffIterator",
                ITERATOR_CONFIG, "timestamp,50"));

        // When
        sleeper.compaction().forceCreateJobs(1);

        // Then
        assertThat(sleeper.query().byQueue().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows().iterableOverRange(50, 100));
    }

    @Test
    void shouldApplyTableIteratorFromUserJarDuringQuery(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));
        sleeper.updateTableProperties(Map.of(
                ITERATOR_CLASS_NAME, "sleeper.example.iterator.FixedAgeOffIterator",
                ITERATOR_CONFIG, "timestamp,50"));

        // When
        List<Row> rows = sleeper.query().byQueue().allRowsInTable();

        // Then
        assertThat(rows).containsExactlyInAnyOrderElementsOf(
                sleeper.generateNumberedRows().iterableOverRange(50, 100));
    }

    @Test
    void shouldApplyQueryIteratorFromUserJar(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When
        List<Row> rows = sleeper.query().byQueue().allRowsWithProcessingConfig(builder -> builder
                .queryTimeIteratorClassName("sleeper.example.iterator.FixedAgeOffIterator")
                .queryTimeIteratorConfig("timestamp,50"));

        // Then
        assertThat(rows).containsExactlyInAnyOrderElementsOf(
                sleeper.generateNumberedRows().iterableOverRange(50, 100));
    }

}
