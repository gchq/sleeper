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
package sleeper.compaction.core.job;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.impl.AgeOffIterator;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobSerDeTest {

    private final CompactionJobSerDe serDe = new CompactionJobSerDe();

    @Test
    void shouldConvertCompactionJobToFromJsonWithNoIterator() {
        // Given
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("test-partition")
                .build();

        // When
        String json = serDe.toJsonPrettyPrint(job);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(job);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldConvertCompactionJobToFromJsonWithIterator() {
        // Given
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("test-partition")
                .iteratorClassName(AgeOffIterator.class.getName())
                .iteratorConfig("key,600000")
                .build();

        // When
        String json = serDe.toJsonPrettyPrint(job);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(job);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldConvertCompactionJobBatchToFromJson() {
        // Given
        CompactionJob job1 = CompactionJob.builder()
                .tableId("some-table")
                .jobId("some-job")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile1")
                .partitionId("some-partition")
                .build();
        CompactionJob job2 = CompactionJob.builder()
                .tableId("other-table")
                .jobId("other-job")
                .inputFiles(Arrays.asList("file3", "file4"))
                .outputFile("outputfile2")
                .partitionId("other-partition")
                .iteratorClassName(AgeOffIterator.class.getName())
                .iteratorConfig("key,600000")
                .build();

        // When
        String json = serDe.toJsonPrettyPrint(List.of(job1, job2));

        // Then
        assertThat(serDe.batchFromJson(json)).containsExactly(job1, job2);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }
}
