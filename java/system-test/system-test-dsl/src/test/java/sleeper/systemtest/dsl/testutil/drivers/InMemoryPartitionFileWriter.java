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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class InMemoryPartitionFileWriter implements PartitionFileWriter {

    private final InMemoryDataStore data;
    private final Partition partition;
    private final String filename;
    private final List<Record> records = new ArrayList<>();

    private InMemoryPartitionFileWriter(InMemoryDataStore data, Partition partition, String filename) {
        this.data = data;
        this.partition = partition;
        this.filename = filename;
    }

    public static PartitionFileWriterFactory factory(
            InMemoryDataStore data, InstanceProperties instanceProperties, TableProperties tableProperties) {
        String filePathPrefix = instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TABLE_ID);
        return partition -> new InMemoryPartitionFileWriter(
                data, partition, filePathPrefix + "/" + UUID.randomUUID());
    }

    @Override
    public void append(Record record) {
        records.add(record);
    }

    @Override
    public CompletableFuture<FileReference> close() {
        data.addFile(filename, records);
        return CompletableFuture.completedFuture(FileReference.builder()
                .filename(filename)
                .partitionId(partition.getId())
                .numberOfRecords((long) records.size())
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build());
    }

    @Override
    public void abort() {

    }
}
