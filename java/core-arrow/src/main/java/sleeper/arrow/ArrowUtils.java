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

package sleeper.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import sleeper.arrow.schema.SchemaBackedByArrow;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ArrowUtils {
    private ArrowUtils() {
    }

    public static int appendToArrowBuffer(VectorSchemaRoot vectorSchemaRoot, SchemaBackedByArrow schema, Map<String, Object> record) {
        int insertAtRowNo = vectorSchemaRoot.getRowCount();
        for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
            Field arrowField = fieldVector.getField();
            ArrowType arrowType = arrowField.getType();
            if (arrowType.equals(new ArrowType.Int(32, true))) {
                IntVector intVector = (IntVector) fieldVector;
                Integer value = (Integer) record.get(arrowField.getName());
                intVector.setSafe(insertAtRowNo, value);
            } else if (arrowType.equals(new ArrowType.Utf8())) {
                VarCharVector varCharVector = (VarCharVector) fieldVector;
                String value = (String) record.get(arrowField.getName());
                varCharVector.setSafe(insertAtRowNo, value.getBytes(StandardCharsets.UTF_8));
            } else {
                throw new UnsupportedOperationException("Arrow column type " + arrowType + " is not handled");
            }
        }
        vectorSchemaRoot.setRowCount(insertAtRowNo + 1);
        return insertAtRowNo;
    }
}
