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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class SystemTestIngestBatcher {
    private final IngestSourceFilesContext sourceFiles;
    private final IngestBatcherDriver driver;
    private final IngestTasksDriver tasksDriver;
    private final WaitForJobs waitForIngest;
    private final WaitForJobs waitForBulkImport;
    private final PollWithRetriesDriver pollDriver;
    private List<String> createdJobIds = new ArrayList<>();

    public SystemTestIngestBatcher(SystemTestContext context, SystemTestDrivers drivers) {
        sourceFiles = context.sourceFiles();
        driver = drivers.ingestBatcher(context);
        tasksDriver = drivers.ingestTasks(context);
        waitForIngest = drivers.waitForIngest(context);
        waitForBulkImport = drivers.waitForBulkImport(context);
        pollDriver = drivers.pollWithRetries();
    }

    public SystemTestIngestBatcher sendSourceFilesExpectingJobs(int expectedJobs, String... filenames) {
        Set<String> jobIdsBefore = driver.allJobIdsInStore().collect(toUnmodifiableSet());
        driver.sendFiles(sourceFiles.getIngestJobFilesInBucket(Stream.of(filenames)));
        try {
            List<String> newJobIds = pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(5), Duration.ofMinutes(2))
                    .queryUntil("expected jobs are found",
                            () -> driver.allJobIdsInStore().filter(not(jobIdsBefore::contains)).toList(),
                            ids -> ids.size() >= expectedJobs);
            if (newJobIds.size() > expectedJobs) {
                throw new IllegalStateException("More jobs were created than expected, found " + newJobIds.size() + ", expected" + expectedJobs);
            }
            createdJobIds.addAll(newJobIds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return this;
    }

    public SystemTestIngestBatcher waitForStandardIngestTask() {
        tasksDriver.waitForTasksForCurrentInstance().waitUntilOneTaskStartedAJob(createdJobIds, pollDriver);
        return this;
    }

    public SystemTestIngestBatcher waitForIngestJobs() {
        waitForIngest.waitForJobs(createdJobIds);
        return this;
    }

    public SystemTestIngestBatcher waitForBulkImportJobs(PollWithRetries pollWithRetries) {
        waitForBulkImport.waitForJobs(createdJobIds, pollWithRetries);
        return this;
    }

    public void clearStore() {
        driver.clearStore();
    }
}
