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
import sleeper.statestore.dynamodb.DynamoDBAssignJobsToFiles;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class AssignJobsToFilesStateStoreAdapter implements AssignJobToFilesRequest.Client {

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final DynamoDBAssignJobsToFiles dynamoDB;

    public AssignJobsToFilesStateStoreAdapter(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            DynamoDBAssignJobsToFiles dynamoDB) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.dynamoDB = dynamoDB;
    }

    @Override
    public void updateJobStatusOfFiles(List<AssignJobToFilesRequest> jobs) throws StateStoreException {
        Map<String, List<AssignJobToFilesRequest>> requestsByTableId = jobs.stream()
                .collect(Collectors.groupingBy(AssignJobToFilesRequest::getTableId));
        List<TableProperties> byStateStoreTables = new ArrayList<>();
        List<AssignJobToFilesRequest> dynamoDBRequests = new ArrayList<>();
        for (String tableId : requestsByTableId.keySet()) {
            TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            if (stateStore instanceof DynamoDBStateStore) {
                dynamoDBRequests.addAll(requestsByTableId.get(tableId));
            } else {
                byStateStoreTables.add(tableProperties);
            }
        }
        updateByStateStore(byStateStoreTables, requestsByTableId);
        dynamoDB.updateDynamoDB(dynamoDBRequests);
    }

    private void updateByStateStore(List<TableProperties> tables, Map<String, List<AssignJobToFilesRequest>> requestsByTableId) throws StateStoreException {
        for (TableProperties table : tables) {
            StateStore stateStore = stateStoreProvider.getStateStore(table);
            List<AssignJobToFilesRequest> requests = requestsByTableId.get(table.get(TABLE_ID));
            stateStore.atomicallyUpdateEachJobStatusOfFiles(requests);
        }
    }
}
