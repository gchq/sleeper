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
package sleeper.compaction.job;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompactionJobTest {

    private CompactionJob.Builder jobForTable() {
        return CompactionJob.builder()
                .tableName("table").tableId("table-id");
    }

    @Test
    public void testShouldThrowOnDuplicateNames() {
        // Given
        List<String> names = Arrays.asList("file1", "file2", "file3", "file1");
        CompactionJob.Builder jobBuilder = jobForTable()
                .jobId("job-1")
                .inputFiles(names);

        // When / Then
        assertThatThrownBy(jobBuilder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testShouldThrowOnDuplicateNulls() {
        // Given
        List<String> names = new ArrayList<>();
        names.add(null);
        names.add(null);
        CompactionJob.Builder jobBuilder = jobForTable()
                .jobId("job-1")
                .inputFiles(names);

        // When / Then
        assertThatThrownBy(jobBuilder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
