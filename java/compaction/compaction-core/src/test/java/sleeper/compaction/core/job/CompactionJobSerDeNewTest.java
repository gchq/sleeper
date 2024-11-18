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

import org.approvaltests.JsonApprovals;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.impl.AgeOffIterator;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobSerDeNewTest {

    private final CompactionJobSerDeNew serDe = new CompactionJobSerDeNew();

    @Test
    void shouldConvertCompactionJobToFromJsonWithNoIterator() {
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("test-partition")
                .build();

        String json = serDe.toJsonPrettyPrint(job);
        assertThat(serDe.fromJson(json)).isEqualTo(job);
        JsonApprovals.verifyJson(json);
    }

    @Test
    void shouldConvertCompactionJobToFromJsonWithIterator() {
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("test-partition")
                .iteratorClassName(AgeOffIterator.class.getName())
                .iteratorConfig("key,600000")
                .build();

        String json = serDe.toJsonPrettyPrint(job);
        assertThat(serDe.fromJson(json)).isEqualTo(job);
        JsonApprovals.verifyJson(json);
    }
}
