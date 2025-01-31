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
package sleeper.clients;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class EstimateSplitPointsClientIT extends LocalStackTestBase {

    private final String bucketName = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        createBucket(bucketName);
    }

    @Test
    void shouldEstimateSplitPointsFromFileInS3() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        Path dataFile = dataFilePath("file.parquet");
        List<Record> records = List.of(
                new Record(Map.of("key", 1L)),
                new Record(Map.of("key", 2L)),
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 4L)),
                new Record(Map.of("key", 5L)),
                new Record(Map.of("key", 6L)),
                new Record(Map.of("key", 7L)),
                new Record(Map.of("key", 8L)),
                new Record(Map.of("key", 9L)),
                new Record(Map.of("key", 10L)));
        writeRecords(dataFile, schema, records);

        // When
        List<Object> splitPoints = EstimateSplitPointsClient.estimate(
                schema, HADOOP_CONF, 4, 32, List.of(dataFile));

        // Then
        assertThat(splitPoints).containsExactly(3L, 6L, 8L);
    }

    private Path dataFilePath(String filename) {
        return new Path("s3a://" + bucketName + "/" + filename);
    }

    private void writeRecords(Path path, Schema schema, List<Record> records) throws IOException {
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, schema, HADOOP_CONF)) {
            for (Record record : records) {
                writer.write(record);
            }
        }
    }

}
