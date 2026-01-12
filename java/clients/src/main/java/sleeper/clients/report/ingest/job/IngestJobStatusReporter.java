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
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;

import java.util.List;
import java.util.Map;

/**
 * Creates reports on the status of ingest and bulk import jobs. The format and output destination can vary based on the
 * implementation.
 */
public interface IngestJobStatusReporter {

    /**
     * Writes a report on the status of ingest and bulk import jobs.
     *
     * @param statusList             the status updates retrieved from the job tracker
     * @param query                  the type of query made for the report
     * @param queueMessages          the counts of messages on ingest and bulk import queues
     * @param persistentEmrStepCount a map from status of EMR execution step, to the number of steps in that state in
     *                               the persistent cluster
     */
    void report(
            List<IngestJobStatus> statusList, JobQuery.Type query, IngestQueueMessages queueMessages,
            Map<String, Integer> persistentEmrStepCount);

}
