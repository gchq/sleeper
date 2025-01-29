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

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.table.TableFilePaths;
import sleeper.core.util.PollWithRetries;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.testutils.SupplierTestHelper.exampleUUID;
import static sleeper.core.testutils.SupplierTestHelper.numberedUUID;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class StateStoreSnapshotsST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldAddManyFiles(SleeperSystemTest sleeper) throws Exception {
        // Given we create fake references for 10,000 files
        sleeper.tables().createWithProperties("snapshots", DEFAULT_SCHEMA,
                Map.of(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName()));
        String partitionId = exampleUUID("partn", 0);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition(partitionId).buildTree();
        sleeper.partitioning().setPartitions(partitions);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        TableFilePaths filePaths = TableFilePaths.buildDataFilePathPrefix(sleeper.instanceProperties(), sleeper.tableProperties());
        Map<String, Long> recordsByFilename = LongStream
                .range(0, 10_000).mapToObj(i -> i)
                .collect(toMap(i -> filePaths.constructPartitionParquetFilePath(partitionId, numberedUUID("file", i)), i -> i));
        List<FileReference> files = recordsByFilename.entrySet().stream()
                .map(entry -> fileFactory.rootFile(entry.getKey(), entry.getValue()))
                .toList();

        // When we send them to the state store committer as two big transactions
        sleeper.stateStore().fakeCommits()
                .send(StateStoreCommitMessage.addFiles(files.subList(0, 5_000)))
                .send(StateStoreCommitMessage.addFiles(files.subList(5_000, 10_000)));

        // Then a snapshot will be created
        AllReferencesToAllFiles snapshotFiles = sleeper.stateStore().waitForFilesSnapshot(
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(10)),
                snapshot -> snapshot.getFiles().size() == 10_000);
        assertThat(snapshotFiles.recordsByFilename())
                .isEqualTo(recordsByFilename);
        assertThat(sleeper.tableFiles().recordsByFilename())
                .isEqualTo(recordsByFilename);
    }

}
