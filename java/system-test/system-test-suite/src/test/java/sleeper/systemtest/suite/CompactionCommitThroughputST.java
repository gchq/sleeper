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
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.compaction.StreamFakeCompactions;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingLong;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class CompactionCommitThroughputST {
    PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldFakeCompactionCommits(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.tables().createWithProperties("compaction", DEFAULT_SCHEMA,
                Map.of(TABLE_ONLINE, "false"));
        sleeper.partitioning().setPartitions(partitions);
        StreamFakeCompactions compactions = StreamFakeCompactions.builder()
                .numCompactions(200000)
                .generateInputFiles(i -> List.of(fileFactory.rootFile("input-" + i + ".parquet", 100)))
                .generateJobId(i -> "job-" + i)
                .generateOutputFile(i -> fileFactory.rootFile("output-" + i + ".parquet", 100))
                .build();
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            compactions.streamAddFiles().forEach(update(store)::addTransaction);
            compactions.streamAssignJobIds().forEach(update(store)::addTransaction);
        });

        // When
        sleeper.compaction()
                .sendFakeCommits(compactions)
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(5)));

        // Then
        assertThat(sleeper.tableFiles().recordsByFilename())
                .isEqualTo(compactions.streamOutputFiles()
                        .collect(groupingBy(FileReference::getFilename,
                                summingLong(FileReference::getNumberOfRecords))));
    }

}
