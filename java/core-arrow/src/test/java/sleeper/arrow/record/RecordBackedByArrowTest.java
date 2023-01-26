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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import sleeper.arrow.schema.SchemaBackedByArrow;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.arrow.schema.ConverterTestHelper.sleeperField;

public class RecordBackedByArrowTest {
    @Test
    void shouldRetrieveValuesFromArrowBuffer() {
        // Given
        SchemaBackedByArrow schemaBackedByArrow = SchemaBackedByArrow.fromSleeperSchema(Schema.builder()
                .rowKeyFields(sleeperField("field1", new StringType()))
                .sortKeyFields(sleeperField("field2", new StringType()))
                .valueFields(sleeperField("field3", new IntType()))
                .build());

        try (BufferAllocator bufferAllocator = new RootAllocator();
             VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaBackedByArrow.getArrowSchema(), bufferAllocator)) {
            // When
            Record record = new Record();
            record.put("field1", "test1");
            record.put("field2", "test2");
            record.put("field3", 123);
            writeRecord(vectorSchemaRoot, schemaBackedByArrow.getSleeperSchema().getAllFields(), record);

            // Then
            RecordBackedByArrow recordBackedByArrow = RecordBackedByArrow.builder()
                    .vectorSchemaRoot(vectorSchemaRoot)
                    .rowNum(1)
                    .build();

            assertThat(recordBackedByArrow.get("field1"))
                    .isEqualTo("test1");
            assertThat(recordBackedByArrow.get("field2"))
                    .isEqualTo("test2");
            assertThat(recordBackedByArrow.get("field3"))
                    .isEqualTo(123);
        }

    }

    private void writeRecord(VectorSchemaRoot vectorSchemaRoot, List<Field> allFields, Record record) {
        int insertAtRowNo = 0;
        for (int fieldNo = 0; fieldNo < allFields.size(); fieldNo++) {
            Field sleeperField = allFields.get(fieldNo);
            String fieldName = sleeperField.getName();
            Type sleeperType = sleeperField.getType();
            if (sleeperType instanceof IntType) {
                IntVector intVector = (IntVector) vectorSchemaRoot.getVector(fieldNo);
                Integer value = (Integer) record.get(fieldName);
                intVector.setSafe(insertAtRowNo, value);
            } else if (sleeperType instanceof StringType) {
                VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector(fieldNo);
                String value = (String) record.get(fieldName);
                varCharVector.setSafe(insertAtRowNo, value.getBytes(StandardCharsets.UTF_8));
            } else {
                throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
            }
        }
        vectorSchemaRoot.setRowCount(insertAtRowNo + 1);
    }
}
