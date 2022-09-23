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

package sleeper.status.report.compactionjobstatus;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.Partition;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;
import sleeper.status.report.filestatus.FilesStatusReportTest;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobStatusReporterTest {
    private CompactionJobTestDataHelper dataHelper;
    private CompactionJobStatusReporter statusReporter;

    @Before
    public void setup() {
        statusReporter = new CompactionJobStatusReporter(System.out);
        dataHelper = new CompactionJobTestDataHelper();
    }

    @Test
    public void shouldReportCompactionJobStatusCreated() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // Then
        System.out.println(status);
        assertThat(statusReporter.report(Collections.singletonList(status), QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/standardJobCreated.txt"));
    }

    @Test
    public void shouldReportSplittingCompactionJobStatusCreated() throws Exception {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // Then
        System.out.println(status);
        assertThat(statusReporter.report(Collections.singletonList(status), QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/splittingJobCreated.txt"));
    }

    private static String example(String path) throws IOException {
        URL url = FilesStatusReportTest.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }
}
