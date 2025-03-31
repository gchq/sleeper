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
package sleeper.systemtest.dsl.statestore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.InMemorySystemTestDrivers;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryStateStoreCommitter;

import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@InMemoryDslTest
public class SystemTestStateStoreFakeCommitsTest {

    private InMemoryStateStoreCommitter committer;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, InMemorySystemTestDrivers drivers) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
        committer = drivers.stateStoreCommitter();
    }

    @Test
    void shouldSendOneFileCommit(SleeperSystemTest sleeper) throws Exception {
        // When
        sleeper.stateStore().fakeCommits()
                .send(StateStoreCommitMessage.addPartitionFile("root", "file.parquet", 100))
                .waitForCommitLogs();

        // Then
        assertThat(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()))
                .isEqualTo("Unreferenced files: 0\n" +
                        "Referenced files: 1\n" +
                        "File references: 1\n" +
                        "Partition at root: 100 records in file 1\n");
    }

    @Test
    void shouldSendManyFileCommits(SleeperSystemTest sleeper) throws Exception {
        // When
        sleeper.stateStore().fakeCommits()
                .sendBatched(LongStream.rangeClosed(1, 1000)
                        .mapToObj(i -> StateStoreCommitMessage.addPartitionFile("root", "file-" + i + ".parquet", i)))
                .waitForCommitLogs();

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(1000);
    }

    @Test
    void shouldWaitForCommitWhenCommitWasMadeButNoRunStartOrFinishLogsWereMade(SleeperSystemTest sleeper) throws Exception {
        // Given
        committer.setRunCommitterOnSend(sleeper, false);
        SystemTestStateStoreFakeCommits commitsDsl = sleeper.stateStore().fakeCommits();
        commitsDsl.send(StateStoreCommitMessage.addPartitionFile("root", "file.parquet", 100));
        committer.addFakeCommits(sleeper, 1);

        // When / Then
        assertThatCode(() -> commitsDsl.waitForCommitLogs())
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFakeCompactFilesIntoOnePerPartition(SleeperSystemTest sleeper) {
        // Given
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .buildTree();
        sleeper.partitioning().setPartitions(partitions);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        FileReference fileL1 = fileFactory.partitionFile("L", "l1.parquet", 100);
        FileReference fileL2 = fileFactory.partitionFile("L", "l2.parquet", 100);
        FileReference fileR = fileFactory.partitionFile("R", "r.parquet", 100);
        sleeper.stateStore().fakeCommits().setupStateStore(stateStore -> {
            AddFilesTransaction.fromReferences(List.of(fileL1, fileL2, fileR)).synchronousCommit(stateStore);
        });

        // When
        sleeper.stateStore().fakeCommits().compactAllFilesToOnePerPartition();

        // Then
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(files.listFileReferences()).hasSize(2);
        assertThat(files.getFilesWithNoReferences()).hasSize(3);
    }

}
