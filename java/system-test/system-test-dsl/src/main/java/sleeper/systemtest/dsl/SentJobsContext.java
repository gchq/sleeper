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

public class SentJobsContext {
    private final List<String> jobIds = new ArrayList<>();
    private GeneratedIngestSourceFiles lastGeneratedFiles;

    public void addJobId(String jobId) {
        jobIds.add(jobId);
    }

    public void addAllJobIds(List<String> ids) {
        jobIds.addAll(ids);
    }

    public void setJobIds(List<String> ids) {
        jobIds.clear();
        jobIds.addAll(ids);
    }

    public void setLastGeneratedFiles(GeneratedIngestSourceFiles files) {
        this.lastGeneratedFiles = files;
    }

    public List<String> getJobIds() {
        if (jobIds.isEmpty() && lastGeneratedFiles != null) {
            jobIds.addAll(lastGeneratedFiles.getJobIdsFromIndividualFiles());
        }
        return Collections.unmodifiableList(jobIds);
    }
}
