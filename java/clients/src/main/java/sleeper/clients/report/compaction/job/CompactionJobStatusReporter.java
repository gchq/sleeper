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

package sleeper.clients.report.compaction.job;

import sleeper.clients.report.job.query.JobQuery;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.util.List;

/**
 * Creates reports on the status of compaction jobs. The format and output destination can vary based on the
 * implementation.
 */
public interface CompactionJobStatusReporter {

    /**
     * Writes a report on the status of compaction jobs.
     *
     * @param jobStatusList the status updates retrieved from the job tracker
     * @param queryType     the type of query made for the report
     */
    void report(List<CompactionJobStatus> jobStatusList, JobQuery.Type queryType);

}
