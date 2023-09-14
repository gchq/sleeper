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

package sleeper.clients.util;

import org.apache.commons.io.IOUtils;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.io.parquet.record.ParquetRecordWriterFactory.createParquetRecordWriter;

public class ShowPageIndexesIT {
    @TempDir
    private Path tempDir;

    @Test
    void shouldShowPageIndexesForFileWithOneRowKeyFieldAndTenRecords() throws Exception {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("test-key", new StringType()))
                .build();
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(schema);

        Path file = tempDir.resolve("test.parquet");
        try (ParquetWriter<Record> writer = createRecordWriter(file, schema)) {
            LongStream.rangeClosed(1, 10).boxed().forEach(i -> {
                Record record = new Record();
                record.put("test-key", String.format("row-%02d", i));
                try {
                    writer.write(record);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        assertThat(runShowPageIndexes(file.toString()))
                .isEqualTo(example("util/showPageIndexes/oneRowKeyField.txt"));
    }

    private static ParquetWriter<Record> createRecordWriter(Path file, Schema schema) throws IOException {
        return createParquetRecordWriter(new org.apache.hadoop.fs.Path(file.toString()), schema);
    }

    private static String example(String path) throws IOException {
        URL url = ShowPageIndexesIT.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }

    private String runShowPageIndexes(String file) throws Exception {
        ToStringPrintStream output = new ToStringPrintStream();
        new ShowPageIndexes(output.getPrintStream()).run(file);
        return output.toString();
    }
}
