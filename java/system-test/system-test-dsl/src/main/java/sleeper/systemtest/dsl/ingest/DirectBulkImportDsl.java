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

package sleeper.systemtest.dsl.ingest;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SentJobsContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.util.UUID;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class DirectBulkImportDsl {

    private final SentJobsContext sentJobs;
    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext sourceFiles;
    private final DirectBulkImportDriver driver;
    private final WaitForJobs waitForJobs;

    public DirectBulkImportDsl(
            SentJobsContext sentJobs,
            SystemTestInstanceContext instance,
            IngestSourceFilesContext sourceFiles,
            DirectBulkImportDriver driver,
            WaitForJobs waitForJobs) {
        this.sentJobs = sentJobs;
        this.instance = instance;
        this.sourceFiles = sourceFiles;
        this.driver = driver;
        this.waitForJobs = waitForJobs;
    }

    public DirectBulkImportDsl sendSourceFiles(String... files) {
        String jobId = UUID.randomUUID().toString();
        sentJobs.addJobId(jobId);
        TableProperties table = instance.getTableProperties();
        driver.sendJob(BulkImportJob.builder()
                .id(jobId)
                .tableId(table.get(TABLE_ID))
                .tableName(table.get(TABLE_NAME))
                .files(sourceFiles.lastFolderWrittenTo().getIngestJobFilesInBucket(Stream.of(files)))
                .build());
        return this;
    }

    public void waitForJobs(PollWithRetries pollWithRetries) {
        waitForJobs.waitForJobs(sentJobs.getJobIds(), pollWithRetries);
    }
}
