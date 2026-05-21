/*
 * Copyright 2022-2026 Crown Copyright
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

package sleeper.systemtest.dsl.python;

import sleeper.systemtest.dsl.SentJobsContext;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.time.Duration;
import java.util.UUID;

public class PythonBulkImportDsl {
    private final SentJobsContext sentJobs;
    private final IngestByAnyQueueDriver ingestDriver;
    private final WaitForJobs waitForJobs;
    private final PollWithRetriesDriver pollDriver;

    public PythonBulkImportDsl(SystemTestContext context) {
        sentJobs = context.sentJobs();
        SystemTestDrivers drivers = context.instance().adminDrivers();
        ingestDriver = drivers.pythonBulkImport(context);
        waitForJobs = drivers.waitForBulkImport(context);
        pollDriver = drivers.pollWithRetries();
    }

    public PythonBulkImportDsl fromS3(String... files) {
        String jobId = UUID.randomUUID().toString();
        ingestDriver.sendJobWithFiles(jobId, files);
        sentJobs.addJobId(jobId);
        return this;
    }

    public void waitForJobs() {
        waitForJobs.waitForJobs(sentJobs.getJobIds(),
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }
}
