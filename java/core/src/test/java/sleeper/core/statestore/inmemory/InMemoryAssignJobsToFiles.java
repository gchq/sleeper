/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.core.statestore.inmemory;

import sleeper.core.statestore.AssignJobToFilesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;

public class InMemoryAssignJobsToFiles implements AssignJobToFilesRequest.Client {

    private final StateStore stateStore;

    private InMemoryAssignJobsToFiles(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public static AssignJobToFilesRequest.Client from(StateStore stateStore) {
        return new InMemoryAssignJobsToFiles(stateStore);
    }

    @Override
    public void updateJobStatusOfFiles(List<AssignJobToFilesRequest> jobs) throws StateStoreException {
        stateStore.atomicallyUpdateEachJobStatusOfFiles(jobs);
    }
}
