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
package sleeper.core.statestore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileInfoTest {

    @Test
    public void testSettersAndGetters() {
        // Given
        FileInfo fileInfo = FileInfo.wholeFile()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(100L)
                .build();

        // When / Then
        assertThat(fileInfo.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
        assertThat(fileInfo.getPartitionId()).isEqualTo("0");
        assertThat(fileInfo.getFilename()).isEqualTo("abc");
        assertThat(fileInfo.getJobId()).isEqualTo("Job1");
        assertThat(fileInfo.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
    }

    @Test
    public void testEqualsAndHashCode() {
        // Given
        FileInfo fileInfo1 = FileInfo.wholeFile()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(100L)
                .build();
        FileInfo fileInfo2 = FileInfo.wholeFile()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(100L)
                .build();
        FileInfo fileInfo3 = FileInfo.wholeFile()
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("0")
                .filename("abc")
                .jobId("Job3")
                .lastStateStoreUpdateTime(2_000_000L)
                .numberOfRecords(100L)
                .build();

        // When / Then
        assertThat(fileInfo2).isEqualTo(fileInfo1)
                .hasSameHashCodeAs(fileInfo1);
        assertThat(fileInfo3).isNotEqualTo(fileInfo1);
        assertThat(fileInfo3.hashCode()).isNotEqualTo(fileInfo1.hashCode());
    }

    @Test
    void shouldNotCreateFileInfoWithoutFilename() {
        // Given
        FileInfo.Builder builder = FileInfo.wholeFile()
                .partitionId("root")
                .numberOfRecords(100L)
                .fileStatus(FileInfo.FileStatus.ACTIVE);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotCreateFileInfoWithoutPartitionId() {
        // Given
        FileInfo.Builder builder = FileInfo.wholeFile()
                .filename("test.parquet")
                .numberOfRecords(100L)
                .fileStatus(FileInfo.FileStatus.ACTIVE);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotCreateFileInfoWithoutFileStatus() {
        // Given
        FileInfo.Builder builder = FileInfo.wholeFile()
                .partitionId("root")
                .filename("test.parquet")
                .numberOfRecords(100L);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotCreateFileInfoWithoutNumberOfRecordsForActiveFile() {
        // Given
        FileInfo.Builder builder = FileInfo.wholeFile()
                .partitionId("root")
                .filename("test.parquet")
                .fileStatus(FileInfo.FileStatus.ACTIVE);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldCreateFileInfoWithoutNumberOfRecordsForGCFile() {
        // Given
        FileInfo.Builder builder = FileInfo.wholeFile()
                .partitionId("root")
                .filename("test.parquet")
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);

        // When / Then
        assertThatCode(builder::build).doesNotThrowAnyException();
    }
}
