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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.GetStateStoreByTableId;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;

import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;

public class CompactionJobCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitter.class);

    private final CompactionJobStatusStore statusStore;
    private final GetStateStoreByTableId stateStoreProvider;

    public CompactionJobCommitter(CompactionJobStatusStore statusStore, GetStateStoreByTableId stateStoreProvider) {
        this.statusStore = statusStore;
        this.stateStoreProvider = stateStoreProvider;
    }

    public void apply(CompactionJobCommitRequest request) throws StateStoreException {
        CompactionJob job = request.getJob();
        updateStateStoreSuccess(
                job, request.getRecordsWritten(), stateStoreProvider.getByTableId(job.getTableId()));
        statusStore.jobFinished(compactionJobFinished(job, request.buildRecordsProcessedSummary()).taskId(request.getTaskId()).build());
        LOGGER.info("Successfully committed compaction job {} to table with ID {}", job.getId(), job.getTableId());
    }

    public static void updateStateStoreSuccess(
            CompactionJob job,
            long recordsWritten,
            StateStore stateStore) throws StateStoreException {
        FileReference fileReference = FileReference.builder()
                .filename(job.getOutputFile())
                .partitionId(job.getPartitionId())
                .numberOfRecords(recordsWritten)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(
                replaceJobFileReferences(job.getId(), job.getPartitionId(), job.getInputFiles(), fileReference)));
    }
}
