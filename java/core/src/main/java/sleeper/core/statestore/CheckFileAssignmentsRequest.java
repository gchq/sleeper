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
package sleeper.core.statestore;

import java.util.List;
import java.util.Objects;

/**
 * A request to check whether a job is assigned to files on a partition. Used for compaction jobs.
 */
public class CheckFileAssignmentsRequest {

    private final String jobId;
    private final List<String> filenames;
    private final String partitionId;

    private CheckFileAssignmentsRequest(String jobId, List<String> filenames, String partitionId) {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.filenames = filenames;
    }

    /**
     * Creates a request to check whether a job is assigned to files on a partition.
     *
     * @param  jobId       the job ID
     * @param  filenames   the filenames
     * @param  partitionId the partition ID
     * @return             the request
     */
    public static CheckFileAssignmentsRequest isJobAssignedToFilesOnPartition(String jobId, List<String> filenames, String partitionId) {
        return new CheckFileAssignmentsRequest(jobId, filenames, partitionId);
    }

    public String getJobId() {
        return jobId;
    }

    public List<String> getFilenames() {
        return filenames;
    }

    public String getPartitionId() {
        return partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, filenames, partitionId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CheckFileAssignmentsRequest)) {
            return false;
        }
        CheckFileAssignmentsRequest other = (CheckFileAssignmentsRequest) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(filenames, other.filenames) && Objects.equals(partitionId, other.partitionId);
    }

    @Override
    public String toString() {
        return "CheckFileAssignmentsRequest{jobId=" + jobId + ", filenames=" + filenames + ", partitionId=" + partitionId + "}";
    }
}
