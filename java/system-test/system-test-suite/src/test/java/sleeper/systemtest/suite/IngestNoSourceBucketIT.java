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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.INGEST_NO_SOURCE_BUCKET;

@Tag("SystemTest")
@Tag("slow")
public class IngestNoSourceBucketIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportAlways(
            sleeper.reportsForExtension().ingestTasksAndJobs());

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(INGEST_NO_SOURCE_BUCKET);
    }

    @Test
    void shouldIngest1FileFromDataBucket() throws Exception {
        // Given
        sleeper.sourceFilesUsingDataBucket()
                .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));

        // When
        sleeper.ingestUsingDataBucket().byQueue().sendSourceFiles("file.parquet")
                .invokeTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.tableFiles().active()).hasSize(1);
    }
}
