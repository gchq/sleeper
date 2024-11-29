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
package sleeper.systemtest.dsl.gc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.properties.validation.IngestFileWritingStrategy;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.ingest.SystemTestDirectIngest;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
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
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.MAIN;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.partitionsBuilder;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;

@InMemoryDslTest
public class GarbageCollectionTest {
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldGarbageCollectFilesAfterCompaction(SleeperSystemTest sleeper) {
        // Given
        sleeper.setGeneratorOverrides(overrideField(ROW_KEY_FIELD_NAME,
                numberStringAndZeroPadTo(5).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(partitionsBuilder(sleeper)
                .rootFirst("root")
                .splitToNewChildren("root", UUID.randomUUID().toString(), UUID.randomUUID().toString(), "row-50000")
                .buildTree());
        sleeper.updateTableProperties(Map.of(
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString(),
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "10",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 100_000));
        SystemTestDirectIngest ingest = sleeper.ingest().direct(tempDir);
        IntStream.range(0, 1000)
                .mapToObj(i -> numbers.range(i * 100, i * 100 + 100))
                .forEach(range -> ingest.numberedRecords(range));
        sleeper.compaction().createJobs(200).invokeTasks(1).waitForJobs();

        // When
        sleeper.garbageCollection().invoke().waitFor(
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(5)));

        // Then
        assertThat(new HashSet<>(sleeper.query().byQueue().allRecordsInTable()))
                .isEqualTo(setFrom(sleeper.generateNumberedRecords(LongStream.range(0, 100_000))));
        assertThat(sleeper.tableFiles().all()).satisfies(files -> {
            assertThat(files.getFilesWithNoReferences()).isEmpty();
            assertThat(files.listFileReferences()).hasSize(200);
        });
    }

    private static <T> Set<T> setFrom(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(toSet());
    }
}
