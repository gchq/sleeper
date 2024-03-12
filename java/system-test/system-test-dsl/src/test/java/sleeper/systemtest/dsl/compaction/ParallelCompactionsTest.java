/*
 * Copyright 2022-2024 Crown Copyright
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.PARALLEL_COMPACTIONS;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.ROW_KEY_FIELD_NAME;

@InMemoryDslTest
@Disabled("TODO")
public class ParallelCompactionsTest {
    private final Schema schema = DEFAULT_SCHEMA;
    public static final int NUMBER_OF_COMPACTIONS = 5;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) throws Exception {
        sleeper.connectToInstance(PARALLEL_COMPACTIONS);
    }

    @Test
    void shouldApplyOneCompactionPerPartition(SleeperSystemTest sleeper) {
        // Given we have partitions split evenly across the intended range of records
        sleeper.setGeneratorOverrides(overrideField(ROW_KEY_FIELD_NAME,
                numberStringAndZeroPadTo(3).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(new PartitionsBuilder(schema)
                .leavesWithSplits(
                        IntStream.range(0, NUMBER_OF_COMPACTIONS)
                                .mapToObj(i -> "" + i)
                                .collect(toUnmodifiableList()),
                        IntStream.range(1, NUMBER_OF_COMPACTIONS)
                                .map(i -> 10000 * i / NUMBER_OF_COMPACTIONS)
                                .mapToObj(i -> numberStringAndZeroPadTo(3, i))
                                .collect(toUnmodifiableList()))
                .anyTreeJoiningAllLeaves()
                .buildTree());
        // And we have records spread across all partitions in two files per partition
        sleeper.ingest().direct(null)
                .numberedRecords(LongStream.range(0, 10000))
                .numberedRecords(LongStream.range(0, 10000));
        // And we configure to compact every partition
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));

        // When we run compaction
        sleeper.compaction().createJobs(NUMBER_OF_COMPACTIONS).waitForJobs();

        // Then we have one output file per compaction
        assertThat(sleeper.tableFiles().references())
                .hasSize(NUMBER_OF_COMPACTIONS);
        // And we have the same records afterwards
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(
                        LongStream.range(0, 10000)
                                .flatMap(i -> LongStream.of(i, i))));
    }

}
