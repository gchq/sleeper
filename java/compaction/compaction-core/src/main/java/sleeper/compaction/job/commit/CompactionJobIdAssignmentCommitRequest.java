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
package sleeper.compaction.job.commit;

import sleeper.compaction.job.CompactionJob;
import sleeper.core.statestore.AssignJobIdRequest;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;

public class CompactionJobIdAssignmentCommitRequest {
    private final String tableId;
    private final List<AssignJobIdRequest> assignJobIdRequests;

    public static CompactionJobIdAssignmentCommitRequest forJobsOnTable(List<CompactionJob> jobs, String tableId) {
        return new CompactionJobIdAssignmentCommitRequest(jobs.stream()
                .map(job -> assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles()))
                .collect(Collectors.toList()),
                tableId);
    }

    public CompactionJobIdAssignmentCommitRequest(List<AssignJobIdRequest> assignJobIdRequests, String tableId) {
        this.tableId = tableId;
        this.assignJobIdRequests = assignJobIdRequests;
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignJobIdRequests);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobIdAssignmentCommitRequest)) {
            return false;
        }
        CompactionJobIdAssignmentCommitRequest other = (CompactionJobIdAssignmentCommitRequest) obj;
        return Objects.equals(assignJobIdRequests, other.assignJobIdRequests);
    }

    @Override
    public String toString() {
        return "CompactionFileAssignmentCommitRequest{assignJobIdRequests=" + assignJobIdRequests + "}";
    }

}
