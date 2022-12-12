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

package sleeper.status.report.ingest.job;

import org.junit.Test;
import sleeper.status.report.job.query.JobQuery;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestJobStatusReportArgumentsTest {
    @Test
    public void shouldSetDefaultArguments() {
        assertThat(IngestJobStatusReportArguments.from("test-instance", "test-table"))
                .usingRecursiveComparison()
                .isEqualTo(IngestJobStatusReportArguments.builder()
                        .instanceId("test-instance").tableName("test-table")
                        .queryType(JobQuery.Type.PROMPT)
                        .reporter(new StandardIngestJobStatusReporter(System.out))
                        .build());
    }
}
