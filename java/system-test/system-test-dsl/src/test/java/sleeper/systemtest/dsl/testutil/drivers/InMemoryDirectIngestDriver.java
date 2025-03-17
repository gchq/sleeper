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

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.record.testutils.InMemoryRecordStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.IngestRecordsFromIterator;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;
import sleeper.ingest.runner.testutils.InMemoryPartitionFileWriter;
import sleeper.ingest.runner.testutils.InMemoryRecordBatch;
import sleeper.ingest.runner.testutils.InMemorySketchesStore;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

public class InMemoryDirectIngestDriver implements DirectIngestDriver {
    private final SystemTestInstanceContext instance;
    private final InMemoryRecordStore data;
    private final InMemorySketchesStore sketches;

    public InMemoryDirectIngestDriver(SystemTestInstanceContext instance, InMemoryRecordStore data, InMemorySketchesStore sketches) {
        this.instance = instance;
        this.data = data;
        this.sketches = sketches;
    }

    @Override
    public void ingest(Path tempDir, Iterator<Record> records) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        TableProperties tableProperties = instance.getTableProperties();
        StateStore stateStore = instance.getStateStore(tableProperties);
        ingest(instanceProperties, tableProperties, stateStore, AddFilesToStateStore.synchronousNoJob(stateStore), records);
    }

    public IngestResult ingest(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            StateStore stateStore, AddFilesToStateStore addFilesToStateStore, Iterator<Record> records) {
        try (IngestCoordinator<Record> coordinator = IngestCoordinator.builderWith(instanceProperties, tableProperties)
                .objectFactory(ObjectFactory.noUserJars())
                .recordBatchFactory(() -> new InMemoryRecordBatch(tableProperties.getSchema()))
                .partitionFileWriterFactory(InMemoryPartitionFileWriter.factory(data, sketches, instanceProperties, tableProperties))
                .stateStore(stateStore)
                .addFilesToStateStore(addFilesToStateStore)
                .build()) {
            return new IngestRecordsFromIterator(coordinator, records).write();
        } catch (IteratorCreationException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
