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

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class StateStoreFileUtils {
    private final Configuration configuration;
    private final Schema schema;

    public StateStoreFileUtils(Schema schema, Configuration configuration) {
        this.schema = schema;
        this.configuration = configuration;
    }

    public void save(String path, Stream<Record> records) throws StateStoreException {
        try (ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(
                new Path(path), schema, configuration)) {
            for (Record record : (Iterable<Record>) () -> records.iterator()) {
                recordWriter.write(record);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed writing records", e);
        }
    }

    public Stream<Record> load(String path) throws StateStoreException {
        List<Record> records = new ArrayList<>();
        try (ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(path), schema)
                .withConf(configuration)
                .build();
                ParquetReaderIterator recordReader = new ParquetReaderIterator(reader)) {
            while (recordReader.hasNext()) {
                records.add(recordReader.next());
            }
        } catch (IOException e) {
            throw new StateStoreException("Failed reading records", e);
        }
        return records.stream();
    }
}
