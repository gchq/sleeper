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
package sleeper.systemtest.dsl.gc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.ingest.DirectIngestDsl;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.partitionsBuilder;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;

@InMemoryDslTest
public class GarbageCollectionTest {
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceNoTables(IN_MEMORY_MAIN);
    }

    @Test
    void shouldGarbageCollectFilesAfterCompaction(SleeperDsl sleeper) {
        // Given
        sleeper.tables().createWithProperties("gc", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString(),
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "10",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        sleeper.setGeneratorOverrides(overrideField(ROW_KEY_FIELD_NAME,
                numberStringAndZeroPadTo(5).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(partitionsBuilder(sleeper)
                .rootFirst("root")
                .splitToNewChildren("root", UUID.randomUUID().toString(), UUID.randomUUID().toString(), "row-50000")
                .buildTree());
        RowNumbers numbers = sleeper.scrambleNumberedRows(LongStream.range(0, 100_000));
        DirectIngestDsl ingest = sleeper.ingest().direct(tempDir);
        IntStream.range(0, 1000)
                .mapToObj(i -> numbers.range(i * 100, i * 100 + 100))
                .forEach(range -> ingest.numberedRows(range));
        sleeper.compaction().createJobs(200).waitForTasks(1).waitForJobs();

        // When
        sleeper.garbageCollection().invoke().waitFor(
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));

        // Then
        assertThat(new HashSet<>(sleeper.query().byQueue().allRowsInTable()))
                .isEqualTo(setFrom(sleeper.generateNumberedRows().iterableOverRange(0, 100_000)));
        assertThat(sleeper.tableFiles().all()).satisfies(files -> {
            assertThat(files.getFilesWithNoReferences()).isEmpty();
            assertThat(files.streamFileReferences()).hasSize(200);
        });
    }

    private static <T> Set<T> setFrom(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(toSet());
    }
}
