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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.runner.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.testutils.InMemorySketchesStore;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class InMemoryPartitionFileWriter implements PartitionFileWriter {
    public static final Logger LOGGER = LoggerFactory.getLogger(InMemoryPartitionFileWriter.class);

    private final InMemoryRowStore dataStore;
    private final InMemorySketchesStore sketchesStore;
    private final Partition partition;
    private final String filename;
    private final List<Row> records = new ArrayList<>();
    private final Sketches sketches;

    private InMemoryPartitionFileWriter(InMemoryRowStore dataStore, InMemorySketchesStore sketchesStore, Partition partition, String filename, Schema schema) {
        this.dataStore = dataStore;
        this.sketchesStore = sketchesStore;
        this.partition = partition;
        this.filename = filename;
        this.sketches = Sketches.from(schema);
    }

    public static PartitionFileWriterFactory factory(
            InMemoryRowStore data, InMemorySketchesStore sketches, InstanceProperties instanceProperties, TableProperties tableProperties) {
        TableFilePaths filePaths = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
        return partition -> new InMemoryPartitionFileWriter(
                data, sketches, partition, filePaths.constructPartitionParquetFilePath(partition, UUID.randomUUID().toString()), tableProperties.getSchema());
    }

    @Override
    public void append(Row record) {
        records.add(record);
        sketches.update(record);
    }

    @Override
    public CompletableFuture<FileReference> close() {
        dataStore.addFile(filename, records);
        sketchesStore.saveFileSketches(filename, sketches);
        LOGGER.info("Wrote file with {} records: {}", records.size(), filename);
        return CompletableFuture.completedFuture(FileReference.builder()
                .filename(filename)
                .partitionId(partition.getId())
                .numberOfRows((long) records.size())
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build());
    }

    @Override
    public void abort() {

    }
}
