/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.dsl;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.systemtest.datageneration.GenerateNumberedRecords;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SystemTestLocalFiles {
    private final SleeperInstanceContext instanceContext;
    private final Path tempDir;

    public SystemTestLocalFiles(SleeperInstanceContext instanceContext, Path tempDir) {
        this.instanceContext = instanceContext;
        this.tempDir = tempDir;
    }

    public SystemTestLocalFiles createWithNumberedRecords(String filename, LongStream numbers) {
        return create(filename, GenerateNumberedRecords.from(instanceContext.getTableProperties().getSchema(), numbers));
    }

    public SystemTestLocalFiles create(String filename, Record... records) {
        return create(filename, Stream.of(records));
    }

    private SystemTestLocalFiles create(String filename, Stream<Record> records) {
        writeFile(instanceContext.getTableProperties(), tempDir.resolve(filename).toString(), records.iterator());
        return this;
    }

    public void writeFile(TableProperties tableProperties, String filePath, Iterator<Record> records) {
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                new org.apache.hadoop.fs.Path("file://" + filePath),
                tableProperties, new Configuration())) {
            for (Record record : (Iterable<Record>) () -> records) {
                writer.write(record);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
