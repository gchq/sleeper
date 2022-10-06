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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class TableWriter {

    private final TableStructure structure;
    private final List<TableField> fields;
    private final List<TableRow> rows;

    private TableWriter(Builder builder) {
        this.structure = builder.structure;
        this.fields = builder.fields;
        this.rows = builder.rows;
    }

    public void write(PrintStream out) {
        int[] maxValueLengthByField = maxValueLengthByField();
        int totalMaxValueLength = Arrays.stream(maxValueLengthByField).sum();
        int maxRowLength = totalMaxValueLength + structure.paddingLengthForFields(fields.size());
        out.println(structure.horizontalBorder(maxRowLength));
        out.println(structure.headerRow(fields, maxValueLengthByField));
        for (TableRow row : rows) {
            out.println(structure.row(row, maxValueLengthByField));
        }
        out.println(structure.horizontalBorder(maxRowLength));
    }

    private int[] maxValueLengthByField() {
        return IntStream.range(0, fields.size())
                .map(this::maxValueLengthByFieldAtIndex)
                .toArray();
    }

    private int maxValueLengthByFieldAtIndex(int index) {
        int headerLength = fields.get(index).getHeader().length();
        return Math.max(headerLength, rows.stream()
                .mapToInt(row -> row.getValueLength(index))
                .max().orElse(0));
    }

    public static final class Builder {
        private final TableStructure structure;
        private final List<TableField> fields;
        private final List<TableRow> rows = new ArrayList<>();

        Builder(TableStructure structure, List<TableField> fields) {
            this.structure = structure;
            this.fields = fields;
        }

        public <T> Builder itemsAndWriter(List<T> items, BiConsumer<T, TableRow.Builder> writer) {
            items.forEach(item -> itemAndWriter(item, writer));
            return this;
        }

        public <T> Builder itemAndWriter(T item, BiConsumer<T, TableRow.Builder> converter) {
            return row(builder -> converter.accept(item, builder));
        }

        public Builder row(Consumer<TableRow.Builder> config) {
            TableRow.Builder recordBuilder = new TableRow.Builder(fields.size());
            config.accept(recordBuilder);
            rows.add(recordBuilder.build());
            return this;
        }

        public TableWriter build() {
            return new TableWriter(this);
        }
    }
}
