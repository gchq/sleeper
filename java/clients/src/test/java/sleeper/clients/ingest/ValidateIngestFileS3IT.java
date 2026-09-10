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
package sleeper.clients.ingest;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetRowWriterFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

class ValidateIngestFileS3IT extends LocalStackTestBase {
    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void shouldValidateS3PathWithSpacesWithoutChangingTheObject() throws IOException {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();
        TableProperties table = createTestTableProperties(createTestInstanceProperties(), schema);
        table.set(TABLE_NAME, "test-table");
        TablePropertiesStore tables = InMemoryTableProperties.getStore();
        tables.createTable(table);
        java.nio.file.Path file = tempDir.resolve("input.parquet");
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(new Path(file.toUri()), schema)) {
            writer.write(new Row(Map.of("key", 1)));
        }
        String bucket = "validate-ingest-" + UUID.randomUUID();
        String key = "folder/input file.parquet";
        createBucket(bucket);
        try {
            s3Client.putObject(request -> request.bucket(bucket).key(key), RequestBody.fromFile(file));
            String originalTag = s3Client.headObject(request -> request.bucket(bucket).key(key)).eTag();
            ToStringConsoleOutput out = new ToStringConsoleOutput();
            ValidateIngestFile client = new ValidateIngestFile(tables, hadoopConf, out.consoleOut());

            assertThat(client.run("instance", "test-table", "s3://" + bucket + "/" + key)).isTrue();
            assertThat(client.run("instance", "test-table", "s3a://" + bucket + "/" + key)).isTrue();
            assertThat(out.toString()).contains("Schema is compatible with standard ingest");
            assertThat(s3Client.headObject(request -> request.bucket(bucket).key(key)).eTag()).isEqualTo(originalTag);
            assertThat(listObjectKeys(bucket)).containsExactly(key);
        } finally {
            s3Client.deleteObject(request -> request.bucket(bucket).key(key));
            s3Client.deleteBucket(request -> request.bucket(bucket));
        }
    }
}
