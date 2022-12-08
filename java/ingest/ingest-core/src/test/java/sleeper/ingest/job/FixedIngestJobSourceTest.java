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
package sleeper.ingest.job;

import org.junit.Test;
import sleeper.ingest.IngestResult;
import sleeper.statestore.FileInfoTestData;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.statestore.FileInfoTestData.defaultFileOnRootPartition;

public class FixedIngestJobSourceTest {

    @Test
    public void shouldRetrieveSomeJobsAndMonitorResults() throws Exception {
        IngestJob job1 = IngestJob.builder()
                .id("test-job-1").tableName("test-table").files("test-file-1")
                .build();
        IngestJob job2 = IngestJob.builder()
                .id("test-job-2").tableName("test-table").files("test-file-2")
                .build();
        FixedIngestJobSource jobSource = new FixedIngestJobSource(job1, job2);

        jobSource.consumeJobs(callbackMakingDefaultFiles());

        assertThat(jobSource.getIngestResults()).containsExactly(
                expectedDefaultFileIngestResult("test-file-1"),
                expectedDefaultFileIngestResult("test-file-2"));
    }

    @Test
    public void shouldRetrieveNoJobsAndMonitorResults() throws Exception {
        FixedIngestJobSource jobSource = new FixedIngestJobSource();

        jobSource.consumeJobs(callbackMakingDefaultFiles());

        assertThat(jobSource.getIngestResults()).isEmpty();
    }

    private static IngestJobSource.Callback callbackMakingDefaultFiles() {
        return job -> IngestResult.from(job.getFiles().stream()
                .map(FileInfoTestData::defaultFileOnRootPartition)
                .collect(Collectors.toList()));
    }

    private static IngestResult expectedDefaultFileIngestResult(String filename) {
        return IngestResult.from(Collections.singletonList(defaultFileOnRootPartition(filename)));
    }
}
