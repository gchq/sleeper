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

import sleeper.garbagecollector.GarbageCollector;
import sleeper.query.core.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static java.util.stream.Collectors.toUnmodifiableList;

public class InMemoryGarbageCollectionDriver implements GarbageCollectionDriver {

    private final SystemTestInstanceContext instance;
    private final InMemoryDataStore data;

    public InMemoryGarbageCollectionDriver(SystemTestInstanceContext instance, InMemoryDataStore data) {
        this.instance = instance;
        this.data = data;
    }

    @Override
    public void invokeGarbageCollection() {
        GarbageCollector collector = new GarbageCollector(
                data::deleteFile,
                instance.getInstanceProperties(),
                instance.getStateStoreProvider(),
                request -> {
                });
        collector.run(instance.streamTableProperties().collect(toUnmodifiableList()));
    }

}
