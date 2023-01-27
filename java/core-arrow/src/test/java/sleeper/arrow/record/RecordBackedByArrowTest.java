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

package sleeper.arrow.record;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import sleeper.arrow.schema.SchemaBackedByArrow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.arrow.ArrowUtils.appendToArrowBuffer;
import static sleeper.arrow.schema.ConverterTestHelper.arrowField;

public class RecordBackedByArrowTest {
    @Test
    void shouldRetrieveRecordsFromArrowBuffer() {
        // Given
        Schema schema = new Schema(
                List.of(
                        arrowField("rowKey1", new ArrowType.Utf8()),
                        arrowField("sortKey1", new ArrowType.Utf8()),
                        arrowField("column1", new ArrowType.Utf8()),
                        arrowField("column2", new ArrowType.Utf8())
                )
        );
        SchemaBackedByArrow schemaBackedByArrow = SchemaBackedByArrow.fromArrowSchema(schema,
                List.of("rowKey1"), List.of("sortKey1"));

        try (BufferAllocator bufferAllocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, bufferAllocator)) {
            Map<String, Object> map1 = new HashMap<>();
            map1.put("rowKey1", "1");
            map1.put("sortKey1", "2");
            map1.put("column1", "A");
            map1.put("column2", "B");
            int map1Index = appendToArrowBuffer(vectorSchemaRoot, schemaBackedByArrow, map1);
            Map<String, Object> map2 = new HashMap<>();
            map2.put("rowKey1", "3");
            map2.put("sortKey1", "4");
            map2.put("column1", "C");
            map2.put("column2", "D");
            int map2Index = appendToArrowBuffer(vectorSchemaRoot, schemaBackedByArrow, map2);

            RecordBackedByArrow recordBackedByArrow1 = RecordBackedByArrow.builder()
                    .vectorSchemaRoot(vectorSchemaRoot)
                    .rowNum(map1Index).build();
            RecordBackedByArrow recordBackedByArrow2 = RecordBackedByArrow.builder()
                    .vectorSchemaRoot(vectorSchemaRoot)
                    .rowNum(map2Index).build();

            assertThat(recordBackedByArrow1.get("column1"))
                    .isEqualTo("A");
            assertThat(recordBackedByArrow1.get("column2"))
                    .isEqualTo("B");
            assertThat(recordBackedByArrow2.get("column1"))
                    .isEqualTo("C");
            assertThat(recordBackedByArrow2.get("column2"))
                    .isEqualTo("D");
        }
    }
}
