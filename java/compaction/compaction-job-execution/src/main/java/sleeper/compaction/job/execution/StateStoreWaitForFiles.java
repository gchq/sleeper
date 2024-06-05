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
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.statestore.StateStoreProvider;

public class StateStoreWaitForFiles implements WaitForFileAssignment {
    public static final int JOB_ASSIGNMENT_WAIT_ATTEMPTS = 10;
    public static final WaitRange JOB_ASSIGNMENT_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(2, 60);
    private final int jobAssignmentWaitAttempts;
    private final ExponentialBackoffWithJitter jobAssignmentWaitBackoff;
    private final StateStoreProvider stateStoreProvider;
    private final TablePropertiesProvider tablePropertiesProvider;

    public StateStoreWaitForFiles(StateStoreProvider stateStoreProvider, TablePropertiesProvider tablePropertiesProvider) {
        this(JOB_ASSIGNMENT_WAIT_ATTEMPTS, new ExponentialBackoffWithJitter(JOB_ASSIGNMENT_WAIT_RANGE),
                stateStoreProvider, tablePropertiesProvider);
    }

    public StateStoreWaitForFiles(
            int jobAssignmentWaitAttempts, ExponentialBackoffWithJitter jobAssignmentWaitBackoff,
            StateStoreProvider stateStoreProvider, TablePropertiesProvider tablePropertiesProvider) {
        this.jobAssignmentWaitAttempts = jobAssignmentWaitAttempts;
        this.jobAssignmentWaitBackoff = jobAssignmentWaitBackoff;
        this.stateStoreProvider = stateStoreProvider;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    @Override
    public void wait(CompactionJob job) throws InterruptedException {
        StateStore stateStore = stateStoreProvider.getStateStore(tablePropertiesProvider.getById(job.getTableId()));
        for (int attempt = 1; attempt <= jobAssignmentWaitAttempts; attempt++) {
            jobAssignmentWaitBackoff.waitBeforeAttempt(attempt);
            try {
                if (allFilesAssignedToJob(stateStore, job)) {
                    return;
                }
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        }
        throw new TimedOutWaitingForFileAssignmentsException();
    }

    private boolean allFilesAssignedToJob(StateStore stateStore, CompactionJob job) throws StateStoreException {
        return stateStore.getFileReferences().stream()
                .filter(file -> isInputFileForJob(file, job))
                .allMatch(file -> job.getId().equals(file.getJobId()));
    }

    private static boolean isInputFileForJob(FileReference file, CompactionJob job) {
        return job.getInputFiles().contains(file.getFilename()) &&
                job.getPartitionId().equals(file.getPartitionId());
    }

}
