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

package sleeper.systemtest.dsl;

import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFiles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tracks the ingest and bulk import jobs that have been sent in the current system test.
 */
public class SentJobsContext {
    private final List<String> jobIds = new ArrayList<>();
    private GeneratedIngestSourceFiles lastGeneratedFiles;

    /**
     * Track that an ingest or bulk import job has been sent.
     *
     * @param jobId the ID of the job that was sent
     */
    public void addSentJob(String jobId) {
        jobIds.add(jobId);
    }

    /**
     * Track that a batch of ingest or bulk import jobs has been sent.
     *
     * @param ids the IDs of the jobs that were sent
     */
    public void addSentJobs(List<String> ids) {
        jobIds.addAll(ids);
    }

    public void setLastGeneratedFiles(GeneratedIngestSourceFiles files) {
        this.lastGeneratedFiles = files;
    }

    /**
     * Gets the IDs of all ingest and bulk import jobs that have been sent in the current system test.
     *
     * @return the job IDs
     */
    public List<String> getJobIds() {
        if (jobIds.isEmpty() && lastGeneratedFiles != null) {
            jobIds.addAll(lastGeneratedFiles.getJobIdsFromIndividualFiles());
        }
        return Collections.unmodifiableList(jobIds);
    }
}
