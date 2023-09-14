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
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.io.parquet.record.ParquetRecordWriterFactory.createParquetRecordWriter;

public class ShowPageIndexesIT {
    @TempDir
    private Path tempDir;

    @Test
    void shouldShowPageIndexesForFileWithOneRowKeyField() throws Exception {
        // Given
        TableProperties tableProperties = createTableProperties(Schema.builder()
                .rowKeyFields(new Field("test-key", new StringType()))
                .build());
        Path file = tempDir.resolve("test.parquet");
        writeRecords(file, tableProperties, LongStream.rangeClosed(1, 100), (i, record) ->
                record.put("test-key", String.format("row-%03d", i)));

        // When/Then
        assertThat(runShowPageIndexes(file))
                .isEqualTo(example("util/showPageIndexes/oneRowKeyField.txt"));
    }

    @Test
    void shouldShowPageIndexesForFileWithTwoRowKeyFields() throws Exception {
        // Given
        TableProperties tableProperties = createTableProperties(Schema.builder()
                .rowKeyFields(
                        new Field("test-key1", new StringType()),
                        new Field("test-key2", new StringType()))
                .build());
        Path file = tempDir.resolve("test.parquet");
        writeRecords(file, tableProperties, LongStream.rangeClosed(1, 100), (i, record) -> {
            record.put("test-key1", String.format("row1-%03d", i));
            record.put("test-key2", String.format("row2-%03d", i));
        });

        // When/Then
        assertThat(runShowPageIndexes(file))
                .isEqualTo(example("util/showPageIndexes/multipleRowKeyFields.txt"));
    }

    @Test
    void shouldShowPageIndexesForFileWithRowKeySortKeyAndValueFields() throws Exception {
        // Given
        TableProperties tableProperties = createTableProperties(Schema.builder()
                .rowKeyFields(new Field("test-key", new StringType()))
                .sortKeyFields(new Field("test-sort", new IntType()))
                .valueFields(new Field("test-value", new LongType()))
                .build());
        Path file = tempDir.resolve("test.parquet");
        writeRecords(file, tableProperties, LongStream.rangeClosed(1, 100), (i, record) -> {
            record.put("test-key", String.format("row-%03d", i));
            record.put("test-sort", i.intValue());
            record.put("test-value", i * 10L);
        });

        // When/Then
        assertThat(runShowPageIndexes(file))
                .isEqualTo(example("util/showPageIndexes/rowKeySortKeyAndValueFields.txt"));
    }

    @Test
    void shouldShowPageIndexesForFileWithMultiplePages() throws Exception {
        // Given
        TableProperties tableProperties = createTableProperties(Schema.builder()
                .rowKeyFields(new Field("test-key", new StringType()))
                .build());
        tableProperties.set(TableProperty.PAGE_SIZE, "100");
        Path file = tempDir.resolve("test.parquet");
        writeRecords(file, tableProperties, LongStream.rangeClosed(1, 1000), (i, record) ->
                record.put("test-key", String.format("row-%04d", i)));

        // When/Then
        assertThat(runShowPageIndexes(file))
                .isEqualTo(example("util/showPageIndexes/multiplePages.txt"));
    }

    @Test
    void shouldShowPageIndexesForFileWithMultipleRowGroups() throws Exception {
        // Given
        TableProperties tableProperties = createTableProperties(Schema.builder()
                .rowKeyFields(new Field("test-key", new StringType()))
                .build());
        tableProperties.set(TableProperty.ROW_GROUP_SIZE, "1");
        Path file = tempDir.resolve("test.parquet");
        writeRecords(file, tableProperties, LongStream.rangeClosed(1, 1000), (i, record) ->
                record.put("test-key", String.format("row-%04d", i)));

        // When/Then
        assertThat(runShowPageIndexes(file))
                .isEqualTo(example("util/showPageIndexes/multipleRowGroups.txt"));
    }

    private static void writeRecords(Path file, TableProperties tableProperties,
                                     LongStream range, BiConsumer<Long, Record> recordCreator) throws IOException {
        try (ParquetWriter<Record> writer = createRecordWriter(file, tableProperties)) {
            range.boxed().forEach(i -> {
                Record record = new Record();
                recordCreator.accept(i, record);
                try {
                    writer.write(record);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    private static TableProperties createTableProperties(Schema schema) {
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    private static ParquetWriter<Record> createRecordWriter(Path file, TableProperties tableProperties) throws IOException {
        return createParquetRecordWriter(new org.apache.hadoop.fs.Path(file.toString()), tableProperties, new Configuration());
    }

    private static String example(String path) throws IOException {
        URL url = ShowPageIndexesIT.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }

    private String runShowPageIndexes(Path file) throws Exception {
        ToStringPrintStream output = new ToStringPrintStream();
        new ShowPageIndexes(output.getPrintStream()).run(file.toString());
        return output.toString();
    }
}
