/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.parquet.row;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;

/**
 * Iterates through rows read from a Parquet file, without knowing the schema of the file in advance. The fields in
 * each row are taken from the schema held in the Parquet file. This can read files that do not match the schema of a
 * Sleeper table, e.g. query results that contain a subset of the fields, or fields computed by the query.
 */
public class ParquetGenericRowIterator implements CloseableIterator<Row> {
    private final ParquetReader<GenericRecord> reader;
    private final GenericRowReadSupport readSupport = new GenericRowReadSupport();
    private GenericRecord next;

    public ParquetGenericRowIterator(Path path, Configuration conf) throws IOException {
        Configuration readConf = new Configuration(conf);
        // Read lists as lists of their elements, rather than lists of records that each wrap an element
        readConf.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
        readConf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        reader = ParquetReader.builder(readSupport, path).withConf(readConf).build();
        try {
            next = reader.read();
        } catch (IOException | RuntimeException e) {
            reader.close();
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Row next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        Row row = AvroRowConverter.toRow(next, readSupport.getFileSchema());
        try {
            next = reader.read();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading from Parquet file", e);
        }
        return row;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
