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
package sleeper.ingest.runner.testutils;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.sketches.testutils.InMemorySketchesStore;

public class InMemoryIngest {

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final StateStore stateStore;
    private final InMemoryRowStore recordStore;
    private final InMemorySketchesStore sketchesStore;

    public InMemoryIngest(InstanceProperties instanceProperties, TableProperties tableProperties,
            StateStore stateStore, InMemoryRowStore recordStore, InMemorySketchesStore sketchesStore) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.stateStore = stateStore;
        this.recordStore = recordStore;
        this.sketchesStore = sketchesStore;
    }

    public IngestCoordinator<Row> createCoordinator() {
        return coordinatorBuilder().build();
    }

    public IngestCoordinator.Builder<Row> coordinatorBuilder() {
        return IngestCoordinator.builderWith(instanceProperties, tableProperties)
                .objectFactory(ObjectFactory.noUserJars())
                .recordBatchFactory(() -> new InMemoryRecordBatch(tableProperties.getSchema()))
                .partitionFileWriterFactory(InMemoryPartitionFileWriter.factory(recordStore, sketchesStore, instanceProperties, tableProperties))
                .stateStore(stateStore);
    }

}
