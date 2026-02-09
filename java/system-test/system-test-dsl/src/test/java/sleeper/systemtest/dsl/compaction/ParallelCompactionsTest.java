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
package sleeper.systemtest.dsl.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;

@InMemoryDslTest
public class ParallelCompactionsTest {
    public static final int NUMBER_OF_COMPACTIONS = 5;
    private final Schema schema = DEFAULT_SCHEMA;
    private final Path tempDir = null;

    @BeforeEach
    void setUp(SleeperDsl sleeper) throws Exception {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldApplyOneCompactionPerPartition(SleeperDsl sleeper) {
        // Given we have partitions split evenly across the intended range of rows
        sleeper.setGeneratorOverrides(overrideField(ROW_KEY_FIELD_NAME,
                numberStringAndZeroPadTo(4).then(addPrefix("row-"))));
        List<String> leafIds = IntStream.range(0, NUMBER_OF_COMPACTIONS)
                .mapToObj(i -> "" + i)
                .collect(toUnmodifiableList());
        List<Object> splitPoints = IntStream.range(1, NUMBER_OF_COMPACTIONS)
                .map(i -> 10000 * i / NUMBER_OF_COMPACTIONS)
                .mapToObj(i -> "row-" + numberStringAndZeroPadTo(4, i))
                .collect(toUnmodifiableList());
        sleeper.partitioning().setPartitions(PartitionsBuilderSplitsFirst
                .leavesWithSplits(schema, leafIds, splitPoints)
                .anyTreeJoiningAllLeaves()
                .buildTree());
        // And we have rows spread across all partitions in two files per partition
        // And we configure to compact every partition
        sleeper.updateTableProperties(Map.of(
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString(),
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        sleeper.ingest().direct(tempDir)
                .numberedRows(LongStream.range(0, 5000).map(i -> i * 2)) // Evens
                .numberedRows(LongStream.range(0, 5000).map(i -> i * 2 + 1)); // Odds

        // When we run compaction
        sleeper.compaction()
                .createJobs(NUMBER_OF_COMPACTIONS)
                .waitForTasks(NUMBER_OF_COMPACTIONS)
                .waitForJobs();

        // Then we have one output file per compaction
        assertThat(sleeper.tableFiles().references())
                .hasSize(NUMBER_OF_COMPACTIONS);
        // And we have the same rows afterwards
        assertThat(inAnyOrder(sleeper.directQuery().allRowsInTable()))
                .isEqualTo(inAnyOrder(sleeper.generateNumberedRows()
                        .iterableOverRange(0, 10000)));
    }

    private static Map<Row, Integer> inAnyOrder(Iterable<Row> rows) {
        Map<Row, Integer> map = new HashMap<>();
        rows.forEach(row -> map.compute(row, (r, count) -> count == null ? 1 : count + 1));
        return map;
    }

}
