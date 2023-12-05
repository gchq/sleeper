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

package sleeper.statestore;

import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.AssignJobToFilesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AssignJobsToFilesStateStoreAdapter implements AssignJobToFilesRequest.Client {

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;

    public AssignJobsToFilesStateStoreAdapter(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
    }

    @Override
    public void updateJobStatusOfFiles(List<AssignJobToFilesRequest> jobs) throws StateStoreException {
        Map<String, List<AssignJobToFilesRequest>> requestsByTableId = jobs.stream()
                .collect(Collectors.groupingBy(AssignJobToFilesRequest::getTableId));
        for (Map.Entry<String, List<AssignJobToFilesRequest>> entry : requestsByTableId.entrySet()) {
            String tableId = entry.getKey();
            StateStore stateStore = stateStoreProvider.getStateStore(tablePropertiesProvider.getById(tableId));
            List<AssignJobToFilesRequest> requests = entry.getValue();
            for (AssignJobToFilesRequest request : requests) {
                stateStore.atomicallyUpdateJobStatusOfFiles(request);
            }
        }
    }
}
