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
package sleeper.core.row;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;

import java.util.Comparator;
import java.util.List;

/**
 * Compares records by row keys then sort keys.
 */
public class RowComparator implements Comparator<Record> {
    private final Schema schema;

    public RowComparator(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int compare(Record row1, Record row2) {
        int rowKeyDiff = compare(schema.getRowKeyFields(), row1, row2);
        if (rowKeyDiff != 0) {
            return rowKeyDiff;
        }
        return compare(schema.getSortKeyFields(), row1, row2);
    }

    private static int compare(List<Field> fields, Record row1, Record row2) {
        for (Field field : fields) {
            PrimitiveType type = (PrimitiveType) field.getType();
            Object value1 = row1.get(field.getName());
            Object value2 = row2.get(field.getName());
            int diff = type.compare(value1, value2);
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }
}
