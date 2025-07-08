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
package sleeper.parquet.testutils;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.record.Record;
import sleeper.core.record.testutils.SortedRecordsCheck;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SortedParquetFileCheckIT {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void shouldFindRecordsAreSortedWithDifferentValues() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of(
                new Record(Map.of("key", 10L)),
                new Record(Map.of("key", 20L)),
                new Record(Map.of("key", 30L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(
                SortedRecordsCheck.sorted(3));
    }

    @Test
    void shouldFindLastTwoRecordsAreOutOfOrder() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of(
                new Record(Map.of("key", 10L)),
                new Record(Map.of("key", 30L)),
                new Record(Map.of("key", 20L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(
                SortedRecordsCheck.outOfOrderAt(3,
                        new Record(Map.of("key", 30L)),
                        new Record(Map.of("key", 20L))));
    }

    @Test
    void shouldFindNoRecordsAreSorted() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of();

        // When / Then
        assertThat(check(schema, records)).isEqualTo(SortedRecordsCheck.sorted(0));
    }

    private SortedRecordsCheck check(Schema schema, List<Record> records) {
        Path path = writeFile(schema, "test.parquet", records);
        return SortedParquetFileCheck.check(path, schema);
    }

    private Path writeFile(Schema schema, String filename, List<Record> records) {
        Path path = new Path(tempDir.resolve(filename).toString());
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, schema)) {
            for (Record record : records) {
                writer.write(record);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return path;
    }

}
