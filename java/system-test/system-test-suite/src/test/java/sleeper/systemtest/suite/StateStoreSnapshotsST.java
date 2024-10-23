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
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.util.PollWithRetries;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.fixtures.SystemTestSchema.DEFAULT_SCHEMA;

@SystemTest
public class StateStoreSnapshotsST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldAddManyFiles(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.tables().createWithProperties("snapshots", DEFAULT_SCHEMA,
                Map.of(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName()));
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.partitioning().setPartitions(partitions);

        // When
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits()
                .sendBatched(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> fileFactory.rootFile("file-" + i + ".parquet", i))
                        .map(StateStoreCommitMessage::addFile))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then a snapshot will be created
        AllReferencesToAllFiles snapshotFiles = sleeper.stateStore()
                .waitForFilesSnapshot(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(10)),
                        files -> files.getFiles().size() == 1000);
        Map<String, Long> expectedRecordsByFilename = LongStream
                .rangeClosed(1, 1000).mapToObj(i -> i)
                .collect(toMap(i -> "file-" + i + ".parquet", i -> i));
        assertThat(snapshotFiles.recordsByFilename())
                .isEqualTo(expectedRecordsByFilename);
        assertThat(sleeper.tableFiles().recordsByFilename())
                .isEqualTo(expectedRecordsByFilename);
    }

}
