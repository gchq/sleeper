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
package sleeper.core.statestore;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileReferenceTest {

    @Test
    public void testSettersAndGetters() {
        // Given
        FileReference fileReference = FileReference.builder()
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(Instant.ofEpochMilli(1_000_000L))
                .numberOfRecords(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();

        // When / Then
        assertThat(fileReference.getPartitionId()).isEqualTo("0");
        assertThat(fileReference.getFilename()).isEqualTo("abc");
        assertThat(fileReference.getJobId()).isEqualTo("Job1");
        assertThat(fileReference.getLastStateStoreUpdateTime()).isEqualTo(Instant.ofEpochMilli(1_000_000L));
    }

    @Test
    public void testEqualsAndHashCode() {
        // Given
        FileReference fileReference1 = FileReference.builder()
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(Instant.ofEpochMilli(1_000_000L))
                .numberOfRecords(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
        FileReference fileReference2 = FileReference.builder()
                .partitionId("0")
                .filename("abc")
                .jobId("Job1")
                .lastStateStoreUpdateTime(Instant.ofEpochMilli(1_000_000L))
                .numberOfRecords(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
        FileReference fileReference3 = FileReference.builder()
                .partitionId("0")
                .filename("abc")
                .jobId("Job3")
                .lastStateStoreUpdateTime(Instant.ofEpochMilli(2_000_000L))
                .numberOfRecords(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();

        // When / Then
        assertThat(fileReference2).isEqualTo(fileReference1)
                .hasSameHashCodeAs(fileReference1);
        assertThat(fileReference3).isNotEqualTo(fileReference1);
        assertThat(fileReference3.hashCode()).isNotEqualTo(fileReference1.hashCode());
    }

    @Test
    void shouldNotCreateFileReferenceWithoutFilename() {
        // Given
        FileReference.Builder builder = FileReference.builder()
                .partitionId("root")
                .numberOfRecords(100L);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotCreateFileReferenceWithoutPartitionId() {
        // Given
        FileReference.Builder builder = FileReference.builder()
                .filename("test.parquet")
                .numberOfRecords(100L);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotCreateFileReferenceWithoutNumberOfRecords() {
        // Given
        FileReference.Builder builder = FileReference.builder()
                .partitionId("root")
                .filename("test.parquet");

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldReferenceFileInChildPartition() {
        // Given
        FileReference file = FileReference.builder()
                .partitionId("root")
                .filename("test.parquet")
                .numberOfRecords(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();

        // When
        FileReference copy = SplitFileReference.referenceForChildPartition(file, "L");

        // Then
        assertThat(copy).isEqualTo(FileReference.builder()
                .partitionId("L")
                .filename("test.parquet")
                .numberOfRecords(50L)
                .countApproximate(true)
                .onlyContainsDataForThisPartition(false)
                .build());
    }
}
