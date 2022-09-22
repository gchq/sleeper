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
package sleeper.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.Partition;

import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobStatusTest {

    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    public void shouldBuildCompactionJobStartedFromJob() {
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        assertThat(CompactionJobStatus.created(job, updateTime)).isEqualTo(
                CompactionJobStatus.builder().jobId(job.getId())
                        .createdStatus(created -> created
                                .updateTime(updateTime)
                                .partitionId(partition.getId())
                                .inputFilesCount(1))
                        .build());
    }

    @Test
    public void shouldBuildSplittingCompactionJobStartedFromJob() {
        CompactionJob job = dataHelper.singleFileSplittingCompaction("root", "left", "right");
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        assertThat(CompactionJobStatus.created(job, updateTime)).isEqualTo(
                CompactionJobStatus.builder().jobId(job.getId())
                        .createdStatus(created -> created
                                .updateTime(updateTime)
                                .partitionId("root")
                                .inputFilesCount(1)
                                .childPartitionIds(Arrays.asList("left", "right")))
                        .build());
    }
}
