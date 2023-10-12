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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.dsl.ingest.SystemTestIngestType;
import sleeper.systemtest.suite.testutil.PurgeQueueExtension;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper.numberOfRecordsIn;

@Tag("SystemTest")
public class IngestIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportIfTestFailed(
            sleeper.reportsForExtension().ingestTasksAndJobs());
    @RegisterExtension
    public final PurgeQueueExtension purgeQueue = PurgeQueueExtension.purgeIfTestFailed(
            INGEST_JOB_QUEUE_URL, sleeper);

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldIngest1File() throws InterruptedException {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));

        // When
        sleeper.ingest().byQueue().sendSourceFiles("file.parquet")
                .invokeTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.tableFiles().active()).hasSize(1);
    }

    @Test
    void shouldIngest4FilesInOneJob() throws InterruptedException {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200))
                .createWithNumberedRecords("file3.parquet", LongStream.range(200, 300))
                .createWithNumberedRecords("file4.parquet", LongStream.range(300, 400));

        // When
        sleeper.ingest().byQueue().sendSourceFiles("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .invokeTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.tableFiles().active()).hasSize(1);
    }

    @Test
    void shouldIngest4FilesInTwoJobs() throws InterruptedException {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200))
                .createWithNumberedRecords("file3.parquet", LongStream.range(200, 300))
                .createWithNumberedRecords("file4.parquet", LongStream.range(300, 400));

        // When
        sleeper.ingest().byQueue()
                .sendSourceFiles("file1.parquet", "file2.parquet")
                .sendSourceFiles("file3.parquet", "file4.parquet")
                .invokeTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.tableFiles().active()).hasSize(2);
    }

    @ParameterizedTest
    @MethodSource("ingestTypesToTestWithManyRecords")
    void shouldIngest20kRecordsWithIngestType(SystemTestIngestType ingestType) throws InterruptedException {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRecords("file.parquet", LongStream.range(0, 20000));

        // When
        sleeper.ingest().setType(ingestType)
                .byQueue().sendSourceFiles("file.parquet")
                .invokeTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 20000)));
        assertThat(sleeper.tableFiles().active())
                .hasSize(1)
                .matches(files -> numberOfRecordsIn(files) == 20_000L,
                        "contain 20K records");
    }

    private static Stream<Arguments> ingestTypesToTestWithManyRecords() {
        return Stream.of(
                Arguments.of(Named.of("Direct write, backed by Arrow",
                        SystemTestIngestType.directWriteBackedByArrow())),
                Arguments.of(Named.of("Async write, backed by Arrow",
                        SystemTestIngestType.asyncWriteBackedByArrow())),
                Arguments.of(Named.of("Direct write, backed by ArrayList",
                        SystemTestIngestType.directWriteBackedByArrayList())));
    }
}
