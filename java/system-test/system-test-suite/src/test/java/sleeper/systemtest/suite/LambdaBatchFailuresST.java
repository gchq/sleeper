/*
 * Copyright 2022-2026 Crown Copyright
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.compaction.StreamFakeCompactions;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Slow2;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.SystemTestTableMetricsHelper.tableMetrics;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BATCHING;

@SystemTest
// Slow because we need to deploy a separate instance to configure batching sizes over the default of 1.
@Slow2
public class LambdaBatchFailuresST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) {
        // Given an instance configured to process 10 messages in a single lambda invocation
        sleeper.connectToInstanceNoTables(BATCHING);
    }

    @Test
    @Disabled("Failing until issue https://github.com/gchq/sleeper/issues/8363")
    void shouldReportFailureCreatingCompactionJobsForSomeTables(SleeperDsl sleeper) {
        // Given 5 Sleeper tables with files to be compacted
        sleeper.tables().createManyWithProperties(5, DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        sleeper.tables().forEach(() -> {
            String tableId = sleeper.tableProperties().get(TABLE_ID);
            sleeper.sourceFiles().inDataBucket().writeSketches()
                    .createWithNumberedRows(tableId + "-1.parquet", LongStream.range(0, 10))
                    .createWithNumberedRows(tableId + "-2.parquet", LongStream.range(0, 10));
            sleeper.ingest().toStateStore()
                    .addFileOnEveryPartition(tableId + "-1.parquet", 10)
                    .addFileOnEveryPartition(tableId + "-2.parquet", 10);
        });

        // When we trigger compaction creation for the tables, interleaved with non-existent tables
        sleeper.compaction()
                .createJobsForAllInterleavedWithFakeTables(5,
                        List.of("missing-1", "missing-2", "missing-3", "missing-4", "missing-5"))
                .waitForTasks(1)
                .waitForJobs();

        // Then the compactions run
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(5)
                .allSatisfy((table, references) -> assertThat(references).hasSize(1));
        // And the messages for the non-existent tables appear on the dead letter queue
        assertThat(sleeper.deadLetters().drainCompactionCreationTriggerDLQForWholeInstance(5))
                .containsExactlyInAnyOrder("missing-1", "missing-2", "missing-3", "missing-4", "missing-5");
    }

    @Test
    @Disabled("Failing until issue https://github.com/gchq/sleeper/issues/8365")
    void shouldReportFailureCommittingSomeCompactionJobs(SleeperDsl sleeper) {
        // Given a Sleeper table with 5 compaction jobs ready to be committed to the state store
        sleeper.tables().createWithProperties("test", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false"));
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.partitioning().setPartitions(partitions);
        StreamFakeCompactions compactions = StreamFakeCompactions.builder()
                .numCompactions(5)
                .generateInputFiles(i -> List.of(fileFactory.rootFile("input-" + i + ".parquet", 100)))
                .generateJobId(i -> "job-" + i)
                .generateOutputFile(i -> fileFactory.rootFile("output-" + i + ".parquet", 100))
                .build();
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            compactions.streamAddFiles().forEach(update(store)::addTransaction);
            compactions.streamAssignJobIds().forEach(update(store)::addTransaction);
        });

        // When we submit commits for those jobs, interleaved with invalid jobs that do not parse
        sleeper.compaction().sendFakeCommitsInterleavedWithInvalid(compactions,
                List.of("invalid-1", "invalid-2", "invalid-3", "invalid-4", "invalid-5"))
                .waitForJobsToCommit();

        // Then the compactions are committed
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(5)
                .allSatisfy((table, references) -> assertThat(references)
                        .extracting(FileReference::getFilename)
                        .containsExactly("output-1.parquet", "output-2.parquet", "output-3.parquet", "output-4.parquet", "output-5.parquet"));
        // And the messages for the invalid jobs appear on the dead letter queue
        assertThat(sleeper.deadLetters().drainCompactionCommitDLQForWholeInstance(5))
                .containsExactlyInAnyOrder("invalid-1", "invalid-2", "invalid-3", "invalid-4", "invalid-5");
    }

    @Test
    @Disabled("Failing until issue https://github.com/gchq/sleeper/issues/8363")
    void shouldReportFailureRunningGCForSomeTables(SleeperDsl sleeper) {
        // Given 5 Sleeper tables with files ready for GC
        sleeper.tables().createManyWithProperties(5, DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        sleeper.tables().forEach(() -> {
            sleeper.ingest().direct(tempDir)
                    .splitIngests(2, sleeper.scrambleNumberedRows(
                            LongStream.range(0, 10)));
            sleeper.stateStore().fakeCommits().compactAllFilesToOnePerPartition();
        });

        // When we trigger GC for the tables, interleaved with non-existent tables
        sleeper.garbageCollection().invokeForAllInterleavedWithFakeTables(
                List.of("missing-1", "missing-2", "missing-3", "missing-4", "missing-5"))
                .waitFor();

        // Then the files are garbage collected
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(5)
                .allSatisfy((table, references) -> assertThat(references).hasSize(1));
        // And the messages for the non-existent tables appear on the dead letter queue
        assertThat(sleeper.deadLetters().drainGarbageCollectionTriggerDLQForWholeInstance(5))
                .containsExactlyInAnyOrder("missing-1", "missing-2", "missing-3", "missing-4", "missing-5");
    }

    @Test
    @Disabled("Failing until issue https://github.com/gchq/sleeper/issues/8363")
    void shouldReportFailureCreatingSnapshotsForSomeTables(SleeperDsl sleeper) {
        // Given 5 Sleeper tables with state store transactions
        sleeper.tables().createManyWithProperties(5, DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false"));
        sleeper.tables().forEach(() -> {
            PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "split")
                    .buildTree();
            FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
            sleeper.partitioning().setPartitions(partitions);
            sleeper.stateStore().fakeCommits().setupStateStore(store -> {
                update(store).addFile(fileFactory.rootFile("test.parquet", 10));
            });
        });

        // When we trigger snapshot creation for the tables, interleaved with non-existent tables
        sleeper.stateStore().invokeSnapshotCreationForAllInterleavedWithFakeTables(
                List.of("missing-1", "missing-2", "missing-3", "missing-4", "missing-5"));

        // Then the snapshots are created
        sleeper.tables().forEach(() -> {
            sleeper.stateStore()
                    .waitForPartitionsSnapshot(snapshot -> snapshot.getLeafPartitions().size() == 2)
                    .waitForFilesSnapshot(snapshot -> snapshot.getFiles().size() == 1);
        });
        // And the messages for the non-existent tables appear on the dead letter queue
        assertThat(sleeper.deadLetters().drainSnapshotCreationTriggerDLQForWholeInstance(5))
                .containsExactlyInAnyOrder("missing-1", "missing-2", "missing-3", "missing-4", "missing-5");
    }

    @Test
    // @Disabled("Failing until issue https://github.com/gchq/sleeper/issues/8363")
    void shouldReportFailureDeletingSnapshotsForSomeTables(SleeperDsl sleeper) {
        // Given 5 Sleeper tables with state store transactions
        sleeper.tables().createManyWithProperties(5, DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, "0"));
        PartitionsBuilder partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "split");
        PartitionTree initialPartitions = partitions.buildTree();
        sleeper.tables().forEach(() -> {
            sleeper.partitioning().setPartitions(initialPartitions);
            sleeper.stateStore().fakeCommits().setupStateStore(store -> {
                update(store).addFile(FileReferenceFactory.from(initialPartitions).rootFile("test.parquet", 10));
            });
        });
        // And snapshots are created
        sleeper.stateStore().invokeSnapshotCreation();
        sleeper.tables().forEach(() -> {
            sleeper.stateStore()
                    .waitForPartitionsSnapshot(snapshot -> snapshot.getLeafPartitions().size() == 2)
                    .waitForFilesSnapshot(snapshot -> snapshot.getFiles().size() == 1);
        });
        // And we make another, newer snapshot so the previous one can be deleted
        partitions.splitToNewChildren("L", "LL", "LR", "again");
        sleeper.tables().forEach(() -> {
            sleeper.stateStore().fakeCommits().setupStateStore(store -> {
                partitions.applySplit(store, "L");
                update(store).addFile(FileReferenceFactory.from(partitions.buildTree())
                        .partitionFile("LL", "test-LL.parquet", 20));
            });
        });
        sleeper.stateStore().invokeSnapshotCreation();
        sleeper.tables().forEach(() -> {
            sleeper.stateStore()
                    .waitForPartitionsSnapshot(snapshot -> snapshot.getLeafPartitions().size() == 3)
                    .waitForFilesSnapshot(snapshot -> snapshot.getFiles().size() == 2);
        });

        // When we trigger snapshot deletion for the tables, interleaved with non-existent tables
        sleeper.stateStore().invokeSnapshotDeletionForAllInterleavedWithFakeTables(
                List.of("missing-1", "missing-2", "missing-3", "missing-4", "missing-5"));

        // Then the snapshots are deleted
        sleeper.tables().forEach(() -> {
            sleeper.stateStore()
                    .waitForPartitionsSnapshotsToDecreaseTo(1)
                    .waitForFilesSnapshotsToDecreaseTo(1);
        });
        // And the messages for the non-existent tables appear on the dead letter queue
        assertThat(sleeper.deadLetters().drainSnapshotDeletionTriggerDLQForWholeInstance(5))
                .containsExactlyInAnyOrder("missing-1", "missing-2", "missing-3", "missing-4", "missing-5");
    }

    @Test
    @Disabled("Failing until issue https://github.com/gchq/sleeper/issues/8363")
    void shouldReportFailureDeletingTransactionsForSomeTables(SleeperDsl sleeper) {
        // Given 5 Sleeper tables configured to delete unneeded transactions immediately
        sleeper.tables().createManyWithProperties(5, DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, "0",
                TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, "0"));
        // And we add transactions to the tables
        sleeper.tables().forEach(() -> {
            PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "split")
                    .buildTree();
            FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
            sleeper.partitioning().setPartitions(partitions);
            sleeper.stateStore().fakeCommits().setupStateStore(store -> {
                update(store).addFile(fileFactory.rootFile("test.parquet", 10));
            });
        });
        // And we create snapshots of the state so that the transactions are no longer needed
        sleeper.stateStore().invokeSnapshotCreation();
        sleeper.tables().forEach(() -> {
            sleeper.stateStore()
                    .waitForPartitionsSnapshot(snapshot -> snapshot.getLeafPartitions().size() == 2)
                    .waitForFilesSnapshot(snapshot -> snapshot.getFiles().size() == 1);
        });

        // When we trigger transaction deletion for the tables, interleaved with non-existent tables
        sleeper.stateStore().invokeTransactionDeletionForAllInterleavedWithFakeTables(
                List.of("missing-1", "missing-2", "missing-3", "missing-4", "missing-5"));

        // Then the transactions are deleted
        sleeper.tables().forEach(() -> {
            sleeper.stateStore()
                    .waitForNoPartitionTransactions()
                    .waitForNoFileTransactions();
        });
        // And the messages for the non-existent tables appear on the dead letter queue
        assertThat(sleeper.deadLetters().drainTransactionDeletionTriggerDLQForWholeInstance(5))
                .containsExactlyInAnyOrder("missing-1", "missing-2", "missing-3", "missing-4", "missing-5");
    }

    @Test
    @Disabled("Failing until issue https://github.com/gchq/sleeper/issues/8363")
    void shouldReportFailureWritingMetricsForSomeTables(SleeperDsl sleeper) {
        // Given 5 Sleeper tables with file references
        sleeper.tables().createManyWithProperties(5, DEFAULT_SCHEMA,
                Map.of(TABLE_ONLINE, "false"));
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.tables().forEach(() -> {
            sleeper.partitioning().setPartitions(new PartitionsBuilder(DEFAULT_SCHEMA)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50")
                    .buildTree());
            sleeper.ingest().direct(tempDir)
                    .numberedRows(LongStream.range(0, 100))
                    .numberedRows(LongStream.range(0, 23));
        });

        // When we trigger table metrics for the tables, interleaved with non-existent tables
        sleeper.tableMetrics().triggerForAllInterleavedWithFakeTables(
                List.of("missing-1", "missing-2", "missing-3", "missing-4", "missing-5"));

        // Then the metrics are published
        sleeper.tables().forEach(() -> {
            assertThat(sleeper.tableMetrics().get()).isEqualTo(tableMetrics(sleeper)
                    .partitionCount(3).leafPartitionCount(2)
                    .fileCount(2).rowCount(123)
                    .averageFileReferencesPerPartition(1.5)
                    .build());
        });
        // And the messages for the non-existent tables appear on the dead letter queue
        assertThat(sleeper.deadLetters().drainTableMetricsDLQForWholeInstance(5))
                .containsExactlyInAnyOrder("missing-1", "missing-2", "missing-3", "missing-4", "missing-5");
    }

}
