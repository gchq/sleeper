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

import sleeper.compaction.core.job.CompactionJob;

import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;

public class CompactionJobIdAssignmentCommitRequestTestHelper {
    private CompactionJobIdAssignmentCommitRequestTestHelper() {
    }

    public static CompactionJobIdAssignmentCommitRequest requestToAssignFilesToJobs(List<CompactionJob> jobs, String tableId) {
        return new CompactionJobIdAssignmentCommitRequest(jobs.stream()
                .map(job -> assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles()))
                .collect(Collectors.toList()),
                tableId);
    }
}
