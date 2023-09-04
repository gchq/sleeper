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

package sleeper.systemtest.suite.dsl;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.ingest.DirectEmrServerlessDriver;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesDriver;
import sleeper.systemtest.drivers.ingest.WaitForIngestJobsDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class SystemTestDirectEmrServerless {

    private final SleeperInstanceContext instance;
    private final IngestSourceFilesDriver sourceFiles;
    private final DirectEmrServerlessDriver driver;
    private final WaitForIngestJobsDriver waitForJobsDriver;
    private final List<String> sentJobIds = new ArrayList<>();

    public SystemTestDirectEmrServerless(SleeperInstanceContext instance,
                                         IngestSourceFilesDriver sourceFiles,
                                         DirectEmrServerlessDriver driver,
                                         WaitForIngestJobsDriver waitForJobsDriver) {
        this.instance = instance;
        this.sourceFiles = sourceFiles;
        this.driver = driver;
        this.waitForJobsDriver = waitForJobsDriver;
    }

    public SystemTestDirectEmrServerless sendSourceFiles(String... files) {
        String jobId = UUID.randomUUID().toString();
        sentJobIds.add(jobId);
        driver.sendJob(BulkImportJob.builder()
                .id(jobId)
                .tableName(instance.getTableName())
                .files(sourceFiles.getIngestJobFilesInBucket(Stream.of(files)))
                .build());
        return this;
    }

    public void waitForJobs(PollWithRetries pollWithRetries) throws InterruptedException {
        waitForJobsDriver.waitForJobs(sentJobIds, pollWithRetries);
    }
}
