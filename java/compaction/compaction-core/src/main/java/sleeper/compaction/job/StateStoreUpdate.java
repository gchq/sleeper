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
package sleeper.compaction.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

public class StateStoreUpdate {
    private StateStoreUpdate() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobStatusStore.class);

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
        try {
            stateStore.atomicallyReplaceFileReferencesWithNewOne(job.getId(), job.getPartitionId(), job.getInputFiles(), fileReference);
            LOGGER.debug("Updated file references in state store");
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating StateStore (moving input files to ready for GC and creating new active file): {}", e.getMessage());
            throw e;
        }
    }
}
