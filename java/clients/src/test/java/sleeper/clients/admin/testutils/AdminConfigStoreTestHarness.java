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
package sleeper.clients.admin.testutils;

import sleeper.clients.admin.AdminClientStatusStoreFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.task.common.QueueMessageCount;

public interface AdminConfigStoreTestHarness {

    void setInstanceProperties(InstanceProperties properties);

    void saveTableProperties(TableProperties tableProperties);

    String getInstanceId();

    default void setInstanceProperties(
            InstanceProperties instanceProperties, TableProperties... tableProperties) {
        setInstanceProperties(instanceProperties);
        for (TableProperties table : tableProperties) {
            saveTableProperties(table);
        }
    }

    void startClient(AdminClientStatusStoreFactory statusStores, QueueMessageCount.Client queueClient) throws Exception;
}
