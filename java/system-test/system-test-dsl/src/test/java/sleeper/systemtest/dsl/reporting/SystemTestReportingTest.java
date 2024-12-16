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
package sleeper.systemtest.dsl.reporting;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.extension.TestContextFactory;
import sleeper.systemtest.dsl.instance.NoInstanceConnectedException;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.InMemorySystemTestDrivers;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class SystemTestReportingTest {

    private Path tempDir = null;

    @Test
    void shouldReportFinishedIngestJob(SleeperSystemTest sleeper) {
        // Given
        sleeper.connectToInstance(IN_MEMORY_MAIN);
        sleeper.sourceFiles().createWithNumberedRecords("test.parquet", LongStream.of(1, 2, 3));
        sleeper.ingest().byQueue().sendSourceFiles("test.parquet").waitForTask().waitForJobs();

        // When / Then
        assertThat(sleeper.reporting().ingestJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(1),
                        "has one finished job");
    }

    @Test
    void shouldReportFinishedCompactionJob(SleeperSystemTest sleeper) {
        // Given
        sleeper.connectToInstance(IN_MEMORY_MAIN);
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.of(1, 2, 3));
        sleeper.compaction().forceCreateJobs(1).invokeTasks(1).waitForJobs();

        // When / Then
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(1),
                        "has one finished job");
    }

    @Test
    void shouldRunCompactionReportAfterTest(SleeperSystemTest sleeper, SystemTestContext context, InMemorySystemTestDrivers drivers, TestInfo info) {
        // Given
        List<String> fakeOutput = new ArrayList<>();
        drivers.reports().setCompactionReport((out, time) -> fakeOutput.add("This is a test report"));
        AfterTestReports afterTestReports = new AfterTestReports(context);
        afterTestReports.reportAlways(builder -> builder.compactionTasksAndJobs());

        // When
        sleeper.connectToInstance(IN_MEMORY_MAIN);
        afterTestReports.afterTestPassed(TestContextFactory.testContext(info));

        // Then
        assertThat(fakeOutput).containsExactly("This is a test report");
    }

    @Test
    void shouldFailCompactionReportAfterTestNotConnectedToInstance(SystemTestContext context, InMemorySystemTestDrivers drivers, TestInfo info) {
        // Given
        List<String> fakeOutput = new ArrayList<>();
        drivers.reports().setCompactionReport((out, time) -> fakeOutput.add("This is a test report"));
        AfterTestReports afterTestReports = new AfterTestReports(context);
        afterTestReports.reportAlways(builder -> builder.compactionTasksAndJobs());

        // When / Then
        assertThatThrownBy(() -> afterTestReports.afterTestPassed(TestContextFactory.testContext(info)))
                .isInstanceOf(NoInstanceConnectedException.class);
    }
}
