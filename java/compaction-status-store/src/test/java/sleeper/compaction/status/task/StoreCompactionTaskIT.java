/*
 * Copyright 2022 Crown Copyright
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

package sleeper.compaction.status.task;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.status.testutils.DynamoDBCompactionTaskStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfoFactory;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionTaskIT extends DynamoDBCompactionTaskStatusStoreTestBase {
    @Test
    public void shouldReportCompactionTaskCreated() {
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        assertThat(false).isTrue();
    }

    @Test
    public void shouldReportCompactionTaskCompleted() {
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        assertThat(false).isTrue();
    }

    @Test
    public void shouldReportSplittingCompactionTaskCompleted() {
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createSplittingCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId(), "A", "B", "C", 0);

        assertThat(false).isTrue();
    }
}
