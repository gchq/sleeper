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
package sleeper.statestore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.core.partition.Partition;
import sleeper.core.range.RegionSerDe;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class StateStoreFileUtils {
    private final FileReferenceSerDe serDe = new FileReferenceSerDe();
    private final Configuration configuration;
    private final Schema schema;

    public static StateStoreFileUtils forFiles(Configuration configuration) {
        return new StateStoreFileUtils(initialiseFilesSchema(), configuration);
    }

    public static StateStoreFileUtils forPartitions(Configuration configuration) {
        return new StateStoreFileUtils(initialisePartitionSchema(), configuration);
    }

    private StateStoreFileUtils(Schema schema, Configuration configuration) {
        this.schema = schema;
        this.configuration = configuration;
    }

    public void savePartitions(String path, StateStorePartitions partitions, Schema sleeperSchema) throws IOException {
        savePartitions(path, partitions.all(), sleeperSchema);
    }

    public void savePartitions(String path, Collection<Partition> partitions, Schema sleeperSchema) throws IOException {
        RegionSerDe regionSerDe = new RegionSerDe(sleeperSchema);
        save(path, partitions.stream().map(partition -> getRecordFromPartition(partition, regionSerDe)));
    }

    public void saveFiles(String path, StateStoreFiles files) throws IOException {
        saveFiles(path, files.referencedAndUnreferenced());
    }

    public void saveFiles(String path, Stream<AllReferencesToAFile> files) throws IOException {
        save(path, files.map(this::getRecordFromFile));
    }

    private void save(String path, Stream<Record> records) throws IOException {
        try (ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(
                new Path(path), schema, configuration)) {
            for (Record record : (Iterable<Record>) () -> records.iterator()) {
                recordWriter.write(record);
            }
        }
    }

    public void loadPartitions(String path, Schema sleeperSchema, Consumer<Partition> partitionConsumer) throws IOException {
        RegionSerDe regionSerDe = new RegionSerDe(sleeperSchema);
        load(path, record -> partitionConsumer.accept(getPartitionFromRecord(record, regionSerDe)));
    }

    public void loadFiles(String path, Consumer<AllReferencesToAFile> fileConsumer) throws IOException {
        load(path, record -> fileConsumer.accept(getFileFromRecord(record)));
    }

    private void load(String path, Consumer<Record> recordConsumer) throws IOException {
        try (ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(path), schema)
                .withConf(configuration).build();
                ParquetReaderIterator recordReader = new ParquetReaderIterator(reader)) {
            while (recordReader.hasNext()) {
                recordConsumer.accept(recordReader.next());
            }
        }
    }

    public boolean isEmpty(String path) throws IOException {
        try (ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(path), schema)
                .withConf(configuration).build();
                ParquetReaderIterator recordReader = new ParquetReaderIterator(reader)) {
            if (recordReader.hasNext()) {
                return false;
            }
        }
        return true;
    }

    private Record getRecordFromPartition(Partition partition, RegionSerDe regionSerDe) {
        Record record = new Record();
        record.put("partitionId", partition.getId());
        record.put("leafPartition", "" + partition.isLeafPartition()); // TODO Change to boolean once boolean is a supported type
        String parentPartitionId;
        if (null == partition.getParentPartitionId()) {
            parentPartitionId = "null";
        } else {
            parentPartitionId = partition.getParentPartitionId();
        }
        record.put("parentPartitionId", parentPartitionId);
        record.put("childPartitionIds", partition.getChildPartitionIds());
        record.put("region", regionSerDe.toJson(partition.getRegion()));
        record.put("dimension", partition.getDimension());
        return record;
    }

    private Partition getPartitionFromRecord(Record record, RegionSerDe regionSerDe) {
        Partition.Builder partitionBuilder = Partition.builder()
                .id((String) record.get("partitionId"))
                .leafPartition(record.get("leafPartition").equals("true"))
                .childPartitionIds((List<String>) record.get("childPartitionIds"))
                .region(regionSerDe.fromJson((String) record.get("region")))
                .dimension((int) record.get("dimension"));
        String parentPartitionId = (String) record.get("parentPartitionId");
        if (!"null".equals(parentPartitionId)) {
            partitionBuilder.parentPartitionId(parentPartitionId);
        }
        return partitionBuilder.build();
    }

    private Record getRecordFromFile(AllReferencesToAFile file) {
        Record record = new Record();
        record.put("fileName", file.getFilename());
        record.put("referencesJson", serDe.collectionToJson(file.getInternalReferences()));
        record.put("externalReferences", file.getExternalReferenceCount());
        record.put("lastStateStoreUpdateTime", file.getLastStateStoreUpdateTime().toEpochMilli());
        return record;
    }

    private AllReferencesToAFile getFileFromRecord(Record record) {
        List<FileReference> internalReferences = serDe.listFromJson((String) record.get("referencesJson"));
        return AllReferencesToAFile.builder()
                .filename((String) record.get("fileName"))
                .internalReferences(internalReferences)
                .totalReferenceCount((int) record.get("externalReferences") + internalReferences.size())
                .lastStateStoreUpdateTime(Instant.ofEpochMilli((long) record.get("lastStateStoreUpdateTime")))
                .build();
    }

    private static Schema initialisePartitionSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("partitionId", new StringType()))
                .valueFields(
                        new Field("leafPartition", new StringType()),
                        new Field("parentPartitionId", new StringType()),
                        new Field("childPartitionIds", new ListType(new StringType())),
                        new Field("region", new StringType()),
                        new Field("dimension", new IntType()))
                .build();
    }

    private static Schema initialiseFilesSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("fileName", new StringType()))
                .valueFields(
                        new Field("referencesJson", new StringType()),
                        new Field("externalReferences", new IntType()),
                        new Field("lastStateStoreUpdateTime", new LongType()))
                .build();
    }
}
