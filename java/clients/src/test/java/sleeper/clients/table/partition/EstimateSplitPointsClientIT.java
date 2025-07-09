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
package sleeper.clients.table.partition;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.record.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class EstimateSplitPointsClientIT extends LocalStackTestBase {

    private final String bucketName = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        createBucket(bucketName);
    }

    @Test
    void shouldEstimateSplitPointsFromFileInS3() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Row> rows = List.of(
                new Row(Map.of("key", 1L)),
                new Row(Map.of("key", 2L)),
                new Row(Map.of("key", 3L)),
                new Row(Map.of("key", 4L)),
                new Row(Map.of("key", 5L)),
                new Row(Map.of("key", 6L)),
                new Row(Map.of("key", 7L)),
                new Row(Map.of("key", 8L)),
                new Row(Map.of("key", 9L)),
                new Row(Map.of("key", 10L)));
        Path dataFile = dataFilePath("file.parquet");
        writeRows(dataFile, schema, rows);

        // When
        List<Object> splitPoints = EstimateSplitPointsClient.estimate(
                schema, hadoopConf, 4, 100, 32, List.of(dataFile));

        // Then
        assertThat(splitPoints).containsExactly(3L, 6L, 8L);
    }

    @Test
    void shouldLimitNumberOfRecordsToRead() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new IntType());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Row row = new Row();
            row.put("key", i);
            rows.add(row);
        }
        Path dataFile = dataFilePath("file.parquet");
        writeRows(dataFile, schema, rows);

        // When
        List<Object> splitPoints = EstimateSplitPointsClient.estimate(
                schema, hadoopConf, 5, 10, 32, List.of(dataFile));

        // Then
        assertThat(splitPoints).containsExactly(2, 4, 6, 8);
    }

    @Test
    void shouldLimitNumberOfRecordsToReadPerFileWithMultipleFiles() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new IntType());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Row row = new Row();
            row.put("key", i);
            rows.add(row);
        }
        Path dataFile1 = dataFilePath("file1.parquet");
        Path dataFile2 = dataFilePath("file2.parquet");
        writeRows(dataFile1, schema, rows);
        writeRows(dataFile2, schema, rows.subList(10, rows.size()));

        // When
        List<Object> splitPoints = EstimateSplitPointsClient.estimate(
                schema, hadoopConf, 5, 10, 32, List.of(dataFile1, dataFile2));

        // Then
        assertThat(splitPoints).containsExactly(4, 8, 12, 16);
    }

    private Path dataFilePath(String filename) {
        return new Path("s3a://" + bucketName + "/" + filename);
    }

    private void writeRows(Path path, Schema schema, List<Row> rows) throws IOException {
        try (ParquetWriter<Row> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, schema, hadoopConf)) {
            for (Row row : rows) {
                writer.write(row);
            }
        }
    }

}
