/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report.table;

import java.util.Objects;

public class TableRow {

    private final String[] values;

    private TableRow(Builder builder) {
        this.values = copyWithNoNulls(builder.values);
    }

    int getValueLength(int index) {
        return values[index].length();
    }

    String getValue(int index) {
        return values[index];
    }

    private static String[] copyWithNoNulls(String[] values) {
        String[] array = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            if (value == null) {
                value = "";
            }
            array[i] = value;
        }
        return array;
    }

    public static final class Builder {
        private final String[] values;

        Builder(int fieldCount) {
            this.values = new String[fieldCount];
        }

        public Builder value(TableField field, Object value) {
            values[field.getIndex()] = Objects.toString(value, "");
            return this;
        }

        public TableRow build() {
            return new TableRow(this);
        }
    }
}
