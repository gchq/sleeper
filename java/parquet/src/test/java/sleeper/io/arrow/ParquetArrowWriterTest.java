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

package sleeper.io.arrow;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.arrow.record.RecordBackedByArrow;
import sleeper.arrow.schema.SchemaBackedByArrow;
import sleeper.io.parquet.arrow.ParquetArrowWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.arrow.ArrowUtils.appendToArrowBuffer;
import static sleeper.arrow.schema.ConverterTestHelper.arrowFieldNullable;

public class ParquetArrowWriterTest {

    @TempDir
    public java.nio.file.Path folder;

    @Test
    void shouldWriteRecordsBackedByArrowToParquetFile() throws IOException {
        // Given
        Schema arrowSchema = new Schema(
                List.of(
                        arrowFieldNullable("rowKey1", new ArrowType.Utf8()),
                        arrowFieldNullable("sortKey1", new ArrowType.Utf8()),
                        arrowFieldNullable("column1", new ArrowType.Utf8()),
                        arrowFieldNullable("column2", new ArrowType.Utf8())
                )
        );
        SchemaBackedByArrow schema = SchemaBackedByArrow.fromArrowSchema(arrowSchema,
                List.of("rowKey1"), List.of("sortKey1"));
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        List<Map<String, Object>> recordMaps = new ArrayList<>();
        try (BufferAllocator bufferAllocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, bufferAllocator)) {

            // When
            Map<String, Object> map1 = new HashMap<>();
            map1.put("rowKey1", "1");
            map1.put("sortKey1", "2");
            map1.put("column1", "A");
            map1.put("column2", "B");
            int map1Index = appendToArrowBuffer(vectorSchemaRoot, schema, map1);
            Map<String, Object> map2 = new HashMap<>();
            map2.put("rowKey1", "3");
            map2.put("sortKey1", "4");
            map2.put("column1", "C");
            map2.put("column2", "D");
            int map2Index = appendToArrowBuffer(vectorSchemaRoot, schema, map2);

            RecordBackedByArrow record1 = RecordBackedByArrow.builder()
                    .vectorSchemaRoot(vectorSchemaRoot)
                    .rowNum(map1Index).build();
            RecordBackedByArrow record2 = RecordBackedByArrow.builder()
                    .vectorSchemaRoot(vectorSchemaRoot)
                    .rowNum(map2Index).build();


            ParquetWriter<RecordBackedByArrow> writer = new ParquetArrowWriter.Builder(path,
                    new SchemaConverter().fromArrow(arrowSchema).getParquetSchema(), schema).build();

            recordMaps.add(saveRecordToMap(schema, record1));
            recordMaps.add(saveRecordToMap(schema, record2));
            writer.write(record1);
            writer.write(record2);
            writer.close();
        }

        // Then
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
        try (
                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator,
                        NativeMemoryPool.getDefault(), FileFormat.PARQUET, "file://" + path.toUri().toString());
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            Schema fileSchema = datasetFactory.inspect();
            assertThat(fileSchema).isEqualTo(arrowSchema);
            int rowNo = 0;
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    RecordBackedByArrow record = RecordBackedByArrow.builder()
                            .rowNum(rowNo)
                            .vectorSchemaRoot(root)
                            .build();
                    assertThat(saveRecordToMap(schema, record))
                            .isEqualTo(recordMaps.get(rowNo++));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Object> saveRecordToMap(SchemaBackedByArrow schemaBackedByArrow, RecordBackedByArrow recordBackedByArrow) {
        Map<String, Object> recordMap = new HashMap<>();
        for (Field field : schemaBackedByArrow.getArrowSchema().getFields()) {
            recordMap.put(field.getName(), recordBackedByArrow.get(field.getName()));
        }
        return recordMap;
    }
}
