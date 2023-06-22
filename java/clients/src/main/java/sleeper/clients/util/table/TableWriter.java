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
package sleeper.clients.util.table;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableWriter {

    private final TableStructure structure;
    private final List<TableField> fields;
    private final List<TableRow> rows;
    private final Set<Integer> hideFieldIndexes;

    private TableWriter(Builder builder) {
        this.structure = builder.structure;
        this.fields = builder.fields;
        this.rows = builder.rows;
        this.hideFieldIndexes = builder.hideFieldIndexes;
    }

    public void write(PrintStream out) {
        List<TableFieldSummary> fieldSummaries = fieldSummaries();
        int totalMaxValueLength = fieldSummaries.stream().mapToInt(TableFieldSummary::getMaxValueLength).sum();
        int maxRowLength = totalMaxValueLength + structure.paddingLengthForFields(fields.size() - hideFieldIndexes.size());
        out.println(structure.horizontalBorder(maxRowLength));
        out.println(structure.headerRow(fields, fieldSummaries));
        for (TableRow row : rows) {
            out.println(structure.row(row, fieldSummaries));
        }
        out.println(structure.horizontalBorder(maxRowLength));
    }

    private List<TableFieldSummary> fieldSummaries() {
        return IntStream.range(0, fields.size())
                .mapToObj(this::fieldSummaryAtIndex)
                .collect(Collectors.toList());
    }

    private TableFieldSummary fieldSummaryAtIndex(int index) {
        TableField field = fields.get(index);
        if (hideFieldIndexes.contains(index)) {
            return TableFieldSummary.hidden(field);
        }
        int headerLength = field.getHeader().length();
        int maxValueLength = Math.max(headerLength, rows.stream()
                .mapToInt(row -> row.getValueLength(index))
                .max().orElse(0));
        return TableFieldSummary.visibleWithMaxValueLength(field, maxValueLength);
    }

    public static final class Builder {
        private final TableStructure structure;
        private final List<TableField> fields;
        private final TableFieldIndex fieldIndex;
        private final List<TableRow> rows = new ArrayList<>();
        private final Set<Integer> hideFieldIndexes = new HashSet<>();

        Builder(TableStructure structure, List<TableField> fields) {
            this.structure = structure;
            this.fields = fields;
            this.fieldIndex = new TableFieldIndex(fields);
        }

        public <T> Builder itemsAndWriter(List<T> items, BiConsumer<T, TableRow.Builder> writer) {
            items.forEach(item -> itemAndWriter(item, writer));
            return this;
        }

        public <T> Builder itemAndWriter(T item, BiConsumer<T, TableRow.Builder> converter) {
            return row(builder -> converter.accept(item, builder));
        }

        public <T> Builder itemsAndSplittingWriter(List<T> items, BiConsumer<T, Builder> writer) {
            items.forEach(item -> writer.accept(item, this));
            return this;
        }

        public Builder row(Consumer<TableRow.Builder> config) {
            TableRow.Builder recordBuilder = new TableRow.Builder(fields.size(), fieldIndex);
            config.accept(recordBuilder);
            rows.add(recordBuilder.build());
            return this;
        }

        public Builder showField(boolean showField, TableFieldReference fieldReference) {
            TableField field = fieldIndex.getField(fieldReference);
            if (showField) {
                hideFieldIndexes.remove(field.getIndex());
            } else {
                hideFieldIndexes.add(field.getIndex());
            }
            return this;
        }

        public Builder showFields(boolean showFields, List<? extends TableFieldReference> fields) {
            for (TableFieldReference field : fields) {
                showField(showFields, field);
            }
            return this;
        }

        public Builder showFields(boolean showFields, TableFieldReference... fields) {
            return showFields(showFields, Arrays.asList(fields));
        }

        public TableWriter build() {
            return new TableWriter(this);
        }
    }
}
