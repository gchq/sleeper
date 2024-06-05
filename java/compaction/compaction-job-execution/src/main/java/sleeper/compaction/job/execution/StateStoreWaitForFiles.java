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
package sleeper.compaction.job.execution;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.execution.CompactionTask.WaitForFileAssignment;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
import sleeper.statestore.StateStoreProvider;

public class StateStoreWaitForFiles implements WaitForFileAssignment {
    private final PollWithRetries pollWithRetries;
    private final StateStoreProvider stateStoreProvider;
    private final TablePropertiesProvider tablePropertiesProvider;

    public StateStoreWaitForFiles(PollWithRetries pollWithRetries, StateStoreProvider stateStoreProvider, TablePropertiesProvider tablePropertiesProvider) {
        this.pollWithRetries = pollWithRetries;
        this.stateStoreProvider = stateStoreProvider;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    @Override
    public void wait(CompactionJob job) throws InterruptedException {
        StateStore stateStore = stateStoreProvider.getStateStore(tablePropertiesProvider.getById(job.getTableId()));
        pollWithRetries.pollUntil("files assigned to job", () -> {
            try {
                return stateStore.getFileReferences().stream()
                        .filter(file -> isInputFileForJob(file, job))
                        .allMatch(file -> job.getId().equals(file.getJobId()));
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static boolean isInputFileForJob(FileReference file, CompactionJob job) {
        return job.getInputFiles().contains(file.getFilename()) &&
                job.getPartitionId().equals(file.getPartitionId());
    }

}
