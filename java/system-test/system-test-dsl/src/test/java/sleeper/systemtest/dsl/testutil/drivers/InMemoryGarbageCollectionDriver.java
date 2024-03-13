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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.table.InvokeForTableRequest;
import sleeper.garbagecollector.GarbageCollector;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class InMemoryGarbageCollectionDriver implements GarbageCollectionDriver {

    private final SystemTestInstanceContext instance;
    private final InMemoryDataStore data;

    public InMemoryGarbageCollectionDriver(SystemTestInstanceContext instance, InMemoryDataStore data) {
        this.instance = instance;
        this.data = data;
    }

    @Override
    public void invokeGarbageCollectionForInstance() {
        GarbageCollector collector = new GarbageCollector(
                data::deleteFile,
                instance.getInstanceProperties(),
                instance.getTablePropertiesProvider(),
                instance.getStateStoreProvider());
        collector.run(new InvokeForTableRequest(instance.streamTableProperties()
                .map(table -> table.get(TABLE_ID))
                .collect(toUnmodifiableList())));
    }

    @Override
    public void sendGarbageCollection() {
        invokeGarbageCollectionForInstance();
    }
}
