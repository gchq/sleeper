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
package sleeper.clients.report.filestatus;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReferenceFactory;

import java.time.Duration;
import java.util.List;

import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class StandardFileStatusReporterDetailLevelTest extends FilesStatusReportTestBase {
    PartitionTree partitions = new PartitionsBuilder(schema)
            .rootFirst("root")
            .splitToNewChildren("root", "L", "R", "mmm")
            .splitToNewChildren("R", "RL", "RR", "ppp")
            .buildTree();

    @BeforeEach
    void setUp() {
        update(stateStore).initialise(partitions);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        update(stateStore).addFiles(List.of(
                fileFactory.partitionFile("L", "left.parquet", 100),
                fileFactory.partitionFile("RL", "rl-no-compaction.parquet", 200),
                fileFactory.partitionFile("RL", "rl-compaction-1a.parquet", 100),
                fileFactory.partitionFile("RL", "rl-compaction-1b.parquet", 100),
                fileFactory.partitionFile("RL", "rl-compaction-2a.parquet", 100),
                fileFactory.partitionFile("RL", "rl-compaction-2b.parquet", 100),
                fileFactory.partitionFile("root", "gc-1.parquet", 100),
                fileFactory.partitionFile("root", "gc-2.parquet", 100)));
        moveTimeForward(Duration.ofMinutes(1));
        update(stateStore).assignJobIds(List.of(
                AssignJobIdRequest.assignJobOnPartitionToFiles("job-1", "RL", List.of("rl-compaction-1a.parquet", "rl-compaction-1b.parquet")),
                AssignJobIdRequest.assignJobOnPartitionToFiles("job-2", "RL", List.of("rl-compaction-2a.parquet", "rl-compaction-2b.parquet")),
                AssignJobIdRequest.assignJobOnPartitionToFiles("job-gc", "root", List.of("gc-1.parquet", "gc-2.parquet"))));
        moveTimeForward(Duration.ofMinutes(1));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes("job-gc", List.of("gc-1.parquet", "gc-2.parquet"),
                fileFactory.partitionFile("root", "root-output.parquet", 100));
    }

    @Test
    void shouldShowStatsOnly() {
        // When / Then
        Approvals.verify(standardReportString(FileStatusDetailLevel.STATS_ONLY),
                new Options().forFile().withBaseName("detailLevelStatsOnly"));
    }

    @Test
    void shouldShowReferences() {
        // When / Then
        Approvals.verify(standardReportString(FileStatusDetailLevel.REFERENCES),
                new Options().forFile().withBaseName("detailLevelReferences"));
    }

}
