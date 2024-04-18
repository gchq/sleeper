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
package sleeper.statestore.transactionlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;

public class TransactionLogFilesSnapshot {
    private static final Schema FILE_SCHEMA = initialiseFilesSchema();
    private final Configuration configuration;
    private final FileReferenceSerDe serDe = new FileReferenceSerDe();

    TransactionLogFilesSnapshot(Configuration configuration) {
        this.configuration = configuration;
    }

    void save(java.nio.file.Path tempDir, StateStoreFiles state, long lastTransactionNumber) throws StateStoreException {
        String path = createFilesPath(lastTransactionNumber);
        try (ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(
                new Path(tempDir.resolve(path).toString()), FILE_SCHEMA, new Configuration())) {
            for (AllReferencesToAFile file : (Iterable<AllReferencesToAFile>) () -> state.referencedAndUnreferenced().iterator()) {
                recordWriter.write(getRecordFromFile(file));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed writing partitions", e);
        }
    }

    StateStoreFiles load(java.nio.file.Path tempDir, long lastTransactionNumber) throws StateStoreException {
        StateStoreFiles files = new StateStoreFiles();
        try (ParquetReader<Record> reader = new ParquetRecordReader.Builder(
                new Path(tempDir.resolve(createFilesPath(lastTransactionNumber)).toString()), FILE_SCHEMA)
                .withConf(configuration)
                .build();
                ParquetReaderIterator recordReader = new ParquetReaderIterator(reader)) {
            while (recordReader.hasNext()) {
                files.add(getFileFromRecord(recordReader.next()));
            }
        } catch (IOException e) {
            throw new StateStoreException("Failed loading partitions", e);
        }
        return files;
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

    private String createFilesPath(long lastTransactionNumber) throws StateStoreException {
        return "snapshots/" + lastTransactionNumber + "-files.parquet";
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
