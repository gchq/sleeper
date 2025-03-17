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

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.record.testutils.InMemoryRecordStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.IngestRecordsFromIterator;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.function.Supplier;

public class InMemoryIngest {

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final StateStore stateStore;
    private final InMemoryRecordStore recordStore;
    private final InMemorySketchesStore sketchesStore;

    public InMemoryIngest(InstanceProperties instanceProperties, TableProperties tableProperties,
            StateStore stateStore, InMemoryRecordStore recordStore, InMemorySketchesStore sketchesStore) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.stateStore = stateStore;
        this.recordStore = recordStore;
        this.sketchesStore = sketchesStore;
    }

    public IngestResult ingestNoJob(Iterator<Record> records) {
        return ingest(AddFilesToStateStore.synchronousNoJob(stateStore), records);
    }

    public IngestResult ingestWithJob(IngestJobTracker jobTracker, IngestJobRunIds runIds, Supplier<Instant> timeSupplier, Iterator<Record> records) {
        return ingest(AddFilesToStateStore.synchronousWithJob(tableProperties, stateStore, jobTracker, timeSupplier, runIds), records);
    }

    private IngestResult ingest(AddFilesToStateStore addFilesToStateStore, Iterator<Record> records) {
        try (IngestCoordinator<Record> coordinator = IngestCoordinator.builderWith(instanceProperties, tableProperties)
                .objectFactory(ObjectFactory.noUserJars())
                .recordBatchFactory(() -> new InMemoryRecordBatch(tableProperties.getSchema()))
                .partitionFileWriterFactory(InMemoryPartitionFileWriter.factory(recordStore, sketchesStore, instanceProperties, tableProperties))
                .stateStore(stateStore)
                .addFilesToStateStore(addFilesToStateStore)
                .build()) {
            return new IngestRecordsFromIterator(coordinator, records).write();
        } catch (IteratorCreationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
