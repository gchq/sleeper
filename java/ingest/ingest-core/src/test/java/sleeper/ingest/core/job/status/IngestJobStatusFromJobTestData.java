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
import sleeper.ingest.core.job.IngestJob;

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
    public static IngestJobStatus jobStatus(IngestJob job, ProcessRun... runs) {
        return IngestJobStatusTestHelper.jobStatus(job.getId(), runs);
    }

}
