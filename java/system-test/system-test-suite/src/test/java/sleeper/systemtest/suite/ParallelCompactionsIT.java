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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.configuration.properties.validation.IngestFileWritingStrategy;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.fixtures.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestSchema.ROW_KEY_FIELD_NAME;

@SystemTest
public class ParallelCompactionsIT {
    private final Schema schema = DEFAULT_SCHEMA;
    public static final int NUMBER_OF_COMPACTIONS = 5;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) throws Exception {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldApplyOneCompactionPerPartition(SleeperSystemTest sleeper) {
        // Given we have partitions split evenly across the intended range of records
        sleeper.setGeneratorOverrides(overrideField(ROW_KEY_FIELD_NAME,
                numberStringAndZeroPadTo(4).then(addPrefix("row-"))));
        List<String> leafIds = IntStream.range(0, NUMBER_OF_COMPACTIONS)
                .mapToObj(i -> "" + i)
                .collect(toUnmodifiableList());
        List<Object> splitPoints = IntStream.range(1, NUMBER_OF_COMPACTIONS)
                .map(i -> 10000 * i / NUMBER_OF_COMPACTIONS)
                .mapToObj(i -> "row-" + numberStringAndZeroPadTo(4, i))
                .collect(toUnmodifiableList());
        sleeper.partitioning().setPartitions(new PartitionsBuilder(schema)
                .leavesWithSplits(leafIds, splitPoints)
                .anyTreeJoiningAllLeaves()
                .buildTree());
        // And we configure to compact every partition
        sleeper.updateTableProperties(Map.of(
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString(),
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        // And we have records spread across all partitions in two files per partition
        sleeper.ingest().direct(null)
                .numberedRecords(LongStream.range(0, 10000))
                .numberedRecords(LongStream.range(0, 10000));

        // When we run compaction
        sleeper.compaction()
                .forceStartTasks(NUMBER_OF_COMPACTIONS)
                .createJobs(NUMBER_OF_COMPACTIONS)
                .waitForJobs();

        // Then we have one output file per compaction
        assertThat(sleeper.tableFiles().references())
                .hasSize(NUMBER_OF_COMPACTIONS);
        // And we have the same records afterwards
        assertThat(inAnyOrder(sleeper.directQuery().allRecordsInTable()))
                .isEqualTo(inAnyOrder(sleeper.generateNumberedRecords(
                        LongStream.range(0, 10000)
                                .flatMap(i -> LongStream.of(i, i)))));
    }

    private static Set<Record> inAnyOrder(Iterable<Record> records) {
        Set<Record> set = new HashSet<>();
        records.forEach(set::add);
        return set;
    }

}
