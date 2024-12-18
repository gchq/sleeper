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
package sleeper.ingest.core.job.status;

import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.ingest.core.job.IngestJob;

import java.time.Instant;

import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;

/**
 * A helper for creating ingest job statuses for tests.
 */
public class IngestJobStatusFromJobTestData {

    private IngestJobStatusFromJobTestData() {
    }

    /**
     * Creates an ingest job status.
     *
     * @param  job  the ingest job
     * @param  runs the process runs
     * @return      an {@link IngestJobStatus}
     */
    public static IngestJobStatus ingestJobStatus(IngestJob job, ProcessRun... runs) {
        return IngestJobStatusTestHelper.ingestJobStatus(job.getId(), runs);
    }

    /**
     * Creates an ingest job accepted status.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @return                an ingest job accepted status
     */
    public static IngestJobAcceptedStatus ingestAcceptedStatus(IngestJob job, Instant validationTime) {
        return IngestJobAcceptedStatus.from(job.getFileCount(), validationTime, defaultUpdateTime(validationTime));
    }

}
