/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.statestore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FileInfoTest {

    @Test
    public void testSettersAndGetters() {
        // Given
        FileInfo fileInfo = FileInfo.builder()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(1_000_000L)
                .onlyContainsDataForThisPartition(true)
                .build();

        // When / Then
        assertThat(fileInfo.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
        assertThat(fileInfo.getPartitionId()).isEqualTo("0");
        assertThat(fileInfo.getFilename()).isEqualTo("abc");
        assertThat(fileInfo.getJobId()).isEqualTo("Job1");
        assertThat(fileInfo.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        assertThat(fileInfo.doesOnlyContainsDataForThisPartition()).isEqualTo(true);
    }

    @Test
    public void testEqualsAndHashCode() {
        // Given
        FileInfo fileInfo1 = FileInfo.builder()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(1_000_000L)
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(1_000_000L)
                .build();
        FileInfo fileInfo3 = FileInfo.builder()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job3")
                .lastStateStoreUpdateTime(2_000_000L)
                .build();
        FileInfo fileInfo4 = FileInfo.builder()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(1_000_000L)
                .onlyContainsDataForThisPartition(false)
                .build();

        // When / Then
        assertThat(fileInfo2).isEqualTo(fileInfo1)
                .hasSameHashCodeAs(fileInfo1);
        assertThat(fileInfo3).isNotEqualTo(fileInfo1);
        assertThat(fileInfo4).isNotEqualTo(fileInfo1);
        assertThat(fileInfo3.hashCode()).isNotEqualTo(fileInfo1.hashCode());
        assertThat(fileInfo4.hashCode()).isNotEqualTo(fileInfo1.hashCode());
    }
}
