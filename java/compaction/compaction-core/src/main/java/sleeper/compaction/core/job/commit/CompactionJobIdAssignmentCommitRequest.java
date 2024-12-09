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
package sleeper.compaction.core.job.commit;

import sleeper.core.statestore.AssignJobIdRequest;

import java.util.List;
import java.util.Objects;

public class CompactionJobIdAssignmentCommitRequest {
    private final String tableId;
    private final List<AssignJobIdRequest> assignJobIdRequests;

    private CompactionJobIdAssignmentCommitRequest(String tableId, List<AssignJobIdRequest> assignJobIdRequests) {
        this.tableId = tableId;
        this.assignJobIdRequests = assignJobIdRequests;
    }

    public static CompactionJobIdAssignmentCommitRequest tableRequests(String tableId, List<AssignJobIdRequest> requests) {
        return new CompactionJobIdAssignmentCommitRequest(tableId, requests);
    }

    public String getTableId() {
        return tableId;
    }

    public List<AssignJobIdRequest> getAssignJobIdRequests() {
        return assignJobIdRequests;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, assignJobIdRequests);
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
        return Objects.equals(tableId, other.tableId) && Objects.equals(assignJobIdRequests, other.assignJobIdRequests);
    }

    @Override
    public String toString() {
        return "CompactionJobIdAssignmentCommitRequest{tableId=" + tableId + ", assignJobIdRequests=" + assignJobIdRequests + "}";
    }
}
