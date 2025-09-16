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
import java.util.Objects;

/**
 * Compares rows by row keys then sort keys.
 */
public class RowComparator implements Comparator<Row> {
    private final List<Field> compareByFields;

    public RowComparator(Schema schema) {
        this(schema.streamRowKeysThenSortKeys().toList());
    }

    private RowComparator(List<Field> compareByFields) {
        this.compareByFields = compareByFields;
    }

    /**
     * Creates a row comparator comparing by the value of specific fields. Rows are equal if the values are equal for
     * all fields.
     *
     * @param  compareByFields the fields to compare by, in order of precedence
     * @return                 the comparator
     */
    public static RowComparator compareByFields(List<Field> compareByFields) {
        return new RowComparator(Objects.requireNonNull(compareByFields, "compareByFields must not be null"));
    }

    @Override
    public int compare(Row row1, Row row2) {
        return compare(compareByFields, row1, row2);
    }

    private static int compare(List<Field> fields, Row row1, Row row2) {
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
