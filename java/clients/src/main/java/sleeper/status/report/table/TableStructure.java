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

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableStructure {

    public static final TableStructure DEFAULT = TableStructure.builder()
            .paddingBeforeRow("| ")
            .paddingBetweenRows(" | ")
            .paddingAfterRow(" |")
            .horizontalBorderCharacter('-')
            .build();

    public static final TableStructure COMPACT = TableStructure.builder()
            .paddingBeforeRow("|")
            .paddingBetweenRows("|")
            .paddingAfterRow("|")
            .horizontalBorderCharacter('-')
            .build();

    private final String paddingBeforeRow;
    private final String paddingAfterRow;
    private final String paddingBetweenRows;
    private final char horizontalBorderCharacter;

    private TableStructure(Builder builder) {
        paddingBeforeRow = Objects.requireNonNull(builder.paddingBeforeRow, "paddingBeforeRow must not be null");
        paddingAfterRow = Objects.requireNonNull(builder.paddingAfterRow, "paddingAfterRow must not be null");
        paddingBetweenRows = Objects.requireNonNull(builder.paddingBetweenRows, "paddingBetweenRows must not be null");
        horizontalBorderCharacter = builder.horizontalBorderCharacter;
    }

    int paddingLengthForFields(int fields) {
        return internalPaddingLengthForFields(fields) + paddingBeforeRow.length() + paddingAfterRow.length();
    }

    private int internalPaddingLengthForFields(int fields) {
        return Math.max(0, fields - 1) * paddingBetweenRows.length();
    }

    String horizontalBorder(int length) {
        return StringUtils.repeat(horizontalBorderCharacter, length);
    }

    String headerRow(List<TableField> fields, int[] maxValueLengthByField, Set<Integer> hideFieldIndexes) {
        return paddedLine(index -> fields.get(index).getHeader(), maxValueLengthByField, hideFieldIndexes);
    }

    String row(TableRow row, int[] maxValueLengthByField, Set<Integer> hideFieldIndexes) {
        return paddedLine(row::getValue, maxValueLengthByField, hideFieldIndexes);
    }

    private String paddedLine(IntFunction<String> getValue, int[] maxValueLengthByField, Set<Integer> hideFieldIndexes) {
        return paddingBeforeRow
                + IntStream.range(0, maxValueLengthByField.length)
                .filter(index -> !hideFieldIndexes.contains(index))
                .mapToObj(index -> paddedValue(getValue.apply(index), maxValueLengthByField[index]))
                .collect(Collectors.joining(paddingBetweenRows))
                + paddingAfterRow;
    }

    private String paddedValue(String value, int maxValueLength) {
        return value + trailingPadding(value, maxValueLength);
    }

    private String trailingPadding(String value, int maxValueLength) {
        return StringUtils.repeat(' ', maxValueLength - value.length());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String paddingBeforeRow;
        private String paddingAfterRow;
        private String paddingBetweenRows;
        private char horizontalBorderCharacter;

        private Builder() {
        }

        public Builder paddingBeforeRow(String paddingBeforeRow) {
            this.paddingBeforeRow = paddingBeforeRow;
            return this;
        }

        public Builder paddingAfterRow(String paddingAfterRow) {
            this.paddingAfterRow = paddingAfterRow;
            return this;
        }

        public Builder paddingBetweenRows(String paddingBetweenRows) {
            this.paddingBetweenRows = paddingBetweenRows;
            return this;
        }

        public Builder horizontalBorderCharacter(char horizontalBorderCharacter) {
            this.horizontalBorderCharacter = horizontalBorderCharacter;
            return this;
        }

        public TableStructure build() {
            return new TableStructure(this);
        }
    }
}
