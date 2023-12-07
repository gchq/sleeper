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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.AssignJobToFilesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

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
        List<TableProperties> s3SleeperTables = new ArrayList<>();
        List<TableProperties> dynamoDBSleeperTables = new ArrayList<>();
        for (String tableId : requestsByTableId.keySet()) {
            TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            if (stateStore instanceof DynamoDBStateStore) {
                dynamoDBSleeperTables.add(tableProperties);
            } else if (stateStore instanceof S3StateStore) {
                s3SleeperTables.add(tableProperties);
            } else {
                throw new IllegalArgumentException("Unsupported state store type: " + stateStore);
            }
        }
        updateByStateStoreNew(s3SleeperTables, requestsByTableId);
        updateByStateStore(dynamoDBSleeperTables, requestsByTableId);
    }

    private void updateByStateStore(List<TableProperties> tables, Map<String, List<AssignJobToFilesRequest>> requestsByTableId) throws StateStoreException {
        for (TableProperties table : tables) {
            StateStore stateStore = stateStoreProvider.getStateStore(table);
            List<AssignJobToFilesRequest> requests = requestsByTableId.get(table.get(TABLE_ID));
            for (AssignJobToFilesRequest request : requests) {
                stateStore.atomicallyUpdateJobStatusOfFiles(request);
            }
        }
    }

    private void updateByStateStoreNew(List<TableProperties> tables, Map<String, List<AssignJobToFilesRequest>> requestsByTableId) throws StateStoreException {
        for (TableProperties table : tables) {
            StateStore stateStore = stateStoreProvider.getStateStore(table);
            List<AssignJobToFilesRequest> requests = requestsByTableId.get(table.get(TABLE_ID));
            stateStore.atomicallyUpdateEachJobStatusOfFiles(requests);
        }
    }
}
