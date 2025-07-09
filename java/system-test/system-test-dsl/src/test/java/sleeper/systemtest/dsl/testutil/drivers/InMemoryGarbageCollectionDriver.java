/*
 * Copyright 2022-2025 Crown Copyright
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

import sleeper.core.row.testutils.InMemoryRecordStore;
import sleeper.garbagecollector.GarbageCollector;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static java.util.stream.Collectors.toUnmodifiableList;

public class InMemoryGarbageCollectionDriver implements GarbageCollectionDriver {

    private final SystemTestInstanceContext instance;
    private final InMemoryRecordStore data;

    public InMemoryGarbageCollectionDriver(SystemTestInstanceContext instance, InMemoryRecordStore data) {
        this.instance = instance;
        this.data = data;
    }

    @Override
    public void invokeGarbageCollection() {
        GarbageCollector collector = new GarbageCollector(
                (filenames, deletedTracker) -> {
                    filenames.forEach(filename -> {
                        data.deleteFile(filename);
                        deletedTracker.deleted(filename);
                    });
                },
                instance.getInstanceProperties(),
                instance.getStateStoreProvider(),
                request -> {
                });
        collector.run(instance.streamTableProperties().collect(toUnmodifiableList()));
    }

}
