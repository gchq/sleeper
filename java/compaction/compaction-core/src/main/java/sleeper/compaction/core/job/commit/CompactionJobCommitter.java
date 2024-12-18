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
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;

import java.util.List;

import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;

public class CompactionJobCommitter {

    private CompactionJobCommitter() {
    }

    public static void updateStateStoreSuccess(
            CompactionJob job,
            long recordsWritten,
            StateStore stateStore) throws ReplaceRequestsFailedException {
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(
                List.of(replaceFileReferencesRequest(job, recordsWritten)));
    }

    public static ReplaceFileReferencesRequest replaceFileReferencesRequest(CompactionJob job, long recordsWritten) {
        FileReference fileReference = FileReference.builder()
                .filename(job.getOutputFile())
                .partitionId(job.getPartitionId())
                .numberOfRecords(recordsWritten)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
        return replaceJobFileReferences(job.getId(), job.getInputFiles(), fileReference);
    }
}
