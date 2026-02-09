/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.clients.report.ingest.job;

import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static sleeper.clients.report.ingest.job.IngestJobStatusReporterTestData.ingestMessageCount;

/**
 * Helpers for tests to create reports on ingest and bulk import jobs based on the job tracker.
 */
public class IngestJobStatusReporterTestHelper {
    private IngestJobStatusReporterTestHelper() {
    }

    /**
     * Creates a report in standard, human readable format, on the status of ingest and bulk import jobs based on state
     * retrieved from the job tracker.
     *
     * @param  query         the type of query used to create the report
     * @param  statusList    the job statuses retrieved from the tracker
     * @param  numberInQueue the number of jobs in the standard ingest job queue
     * @return               the report as a human readable string
     */
    public static String getStandardReport(JobQuery.Type query, List<IngestJobStatus> statusList, int numberInQueue) {
        return getStandardReport(query, statusList, numberInQueue, Collections.emptyMap());
    }

    /**
     * Creates a report in standard, human readable format, on the status of ingest and bulk import jobs based on state
     * retrieved from the job tracker.
     *
     * @param  query                  the type of query used to create the report
     * @param  statusList             the job statuses retrieved from the tracker
     * @param  numberInQueue          the number of jobs in the standard ingest job queue
     * @param  persistentEmrStepCount a map from status of an EMR step, to the number of steps in that status in the
     *                                persistent EMR cluster
     * @return                        the report as a human readable string
     */
    public static String getStandardReport(
            JobQuery.Type query, List<IngestJobStatus> statusList, int numberInQueue,
            Map<String, Integer> persistentEmrStepCount) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        new StandardIngestJobStatusReporter(output.getPrintStream()).report(statusList, query,
                ingestMessageCount(numberInQueue), persistentEmrStepCount);
        return output.toString();
    }

    /**
     * Creates a report in JSON format, on the status of ingest and bulk import jobs based on state retrieved from the
     * job tracker.
     *
     * @param  query         the type of query used to create the report
     * @param  statusList    the job statuses retrieved from the tracker
     * @param  numberInQueue the number of jobs in the standard ingest job queue
     * @return               the report as a human readable string
     */
    public static String getJsonReport(JobQuery.Type query, List<IngestJobStatus> statusList, int numberInQueue) {
        return getJsonReport(query, statusList, numberInQueue, Collections.emptyMap());
    }

    /**
     * Creates a report in JSON format, on the status of ingest and bulk import jobs based on state retrieved from the
     * job tracker.
     *
     * @param  query                  the type of query used to create the report
     * @param  statusList             the job statuses retrieved from the tracker
     * @param  numberInQueue          the number of jobs in the standard ingest job queue
     * @param  persistentEmrStepCount a map from status of an EMR step, to the number of steps in that status in the
     *                                persistent EMR cluster
     * @return                        the report as a human readable string
     */
    public static String getJsonReport(
            JobQuery.Type query, List<IngestJobStatus> statusList, int numberInQueue,
            Map<String, Integer> persistentEmrStepCount) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        new JsonIngestJobStatusReporter(output.getPrintStream()).report(statusList, query,
                ingestMessageCount(numberInQueue), persistentEmrStepCount);
        return output.toString();
    }
}
