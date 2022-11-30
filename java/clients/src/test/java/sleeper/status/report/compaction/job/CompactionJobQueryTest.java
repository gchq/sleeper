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

package sleeper.status.report.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.TestCompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.status.report.compaction.job.CompactionJobStatusReporter.QueryType;

public class CompactionJobQueryTest {
    private static final String tableName = "test-table";
    private final CompactionJobStatusStore statusStore = mock(CompactionJobStatusStore.class);
    private final List<CompactionJobStatus> exampleStatusList = exampleStatusList();

    @Test
    public void shouldCreateAllQueryWithNoParameters() {
        // Given
        QueryType queryType = QueryType.ALL;
        when(statusStore.getAllJobs(tableName)).thenReturn(exampleStatusList);

        // When
        CompactionJobQuery query = CompactionJobQuery.from(tableName, queryType);

        // Then
        assertThat(query.run(statusStore)).isEqualTo(exampleStatusList);
    }

    private List<CompactionJobStatus> exampleStatusList() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        return Arrays.asList(
                TestCompactionJobStatus.created(dataHelper.singleFileCompaction(),
                        Instant.parse("2022-09-22T13:33:12.001Z")),
                TestCompactionJobStatus.created(dataHelper.singleFileCompaction(),
                        Instant.parse("2022-09-22T13:53:12.001Z"))
        );
    }
}
