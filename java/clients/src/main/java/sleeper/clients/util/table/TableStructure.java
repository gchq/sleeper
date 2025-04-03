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
package sleeper.clients.util.table;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public class TableStructure {

    public static final TableStructure DEFAULT = TableStructure.builder()
            .paddingBeforeRow("| ")
            .paddingBetweenColumns(" | ")
            .paddingAfterRow(" |")
            .horizontalBorderCharacter('-')
            .hasHorizontalBorder(true)
            .build();

    public static final TableStructure MARKDOWN_FORMAT = TableStructure.builder()
            .paddingBeforeRow("| ")
            .paddingAfterRow(" |")
            .paddingBetweenColumns(" | ")
            .separatorRowCharacter('-')
            .hasSeparatorBelowHeader(true).build();

    private final String paddingBeforeRow;
    private final String paddingAfterRow;
    private final String paddingBetweenColumns;
    private final char horizontalBorderCharacter;
    private final boolean hasHorizontalBorder;
    private final char separatorRowCharacter;
    private final boolean hasSeparatorBelowHeader;

    private TableStructure(Builder builder) {
        paddingBeforeRow = Objects.requireNonNull(builder.paddingBeforeRow, "paddingBeforeRow must not be null");
        paddingAfterRow = Objects.requireNonNull(builder.paddingAfterRow, "paddingAfterRow must not be null");
        paddingBetweenColumns = Objects.requireNonNull(builder.paddingBetweenColumns, "paddingBetweenColumns must not be null");
        horizontalBorderCharacter = builder.horizontalBorderCharacter;
        hasHorizontalBorder = builder.hasHorizontalBorder;
        separatorRowCharacter = builder.separatorRowCharacter;
        hasSeparatorBelowHeader = builder.hasSeparatorBelowHeader;
    }

    int paddingLengthForFields(int fields) {
        return internalPaddingLengthForFields(fields) + paddingBeforeRow.length() + paddingAfterRow.length();
    }

    private int internalPaddingLengthForFields(int fields) {
        return Math.max(0, fields - 1) * paddingBetweenColumns.length();
    }

    public boolean hasHorizontalBorder() {
        return this.hasHorizontalBorder;
    }

    String horizontalBorder(int length) {
        return StringUtils.repeat(horizontalBorderCharacter, length);
    }

    String headerRow(List<TableField> fields, List<TableFieldSummary> fieldSummaries) {
        String output = paddedLine(index -> fields.get(index).getHeader(), fieldSummaries);

        // Padded line results in extra spaces at start and end column of column for readability.
        // Spaces are replaced with the seperator character to provide full line.
        if (hasSeparatorBelowHeader) {
            output += "\n" + paddedLine(index -> generateFiller(index, fieldSummaries), fieldSummaries)
                    .replace(' ', separatorRowCharacter);
        }
        return output;
    }

    private String generateFiller(int index, List<TableFieldSummary> fieldSummaries) {
        return StringUtils.repeat(separatorRowCharacter, fieldSummaries.get(index).getMaxValueLength());
    }

    String row(TableRow row, List<TableFieldSummary> fieldSummaries) {
        return paddedLine(row::getValue, fieldSummaries);
    }

    private String paddedLine(IntFunction<String> getValue, List<TableFieldSummary> fieldSummaries) {
        return paddingBeforeRow
                + fieldSummaries.stream()
                        .filter(TableFieldSummary::isVisible)
                        .map(field -> paddedValue(getValue.apply(field.getIndex()), field))
                        .collect(Collectors.joining(paddingBetweenColumns))
                + paddingAfterRow;
    }

    private String paddedValue(String value, TableFieldSummary fieldSummary) {
        String padding = valuePadding(value, fieldSummary.getMaxValueLength());
        switch (fieldSummary.getHorizontalAlignment()) {
            case RIGHT:
                return padding + value;
            case LEFT:
            default:
                return value + padding;
        }
    }

    private String valuePadding(String value, int maxValueLength) {
        return StringUtils.repeat(' ', maxValueLength - value.length());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String paddingBeforeRow;
        private String paddingAfterRow;
        private String paddingBetweenColumns;
        private char horizontalBorderCharacter;
        private boolean hasHorizontalBorder = false;
        private char separatorRowCharacter;
        private boolean hasSeparatorBelowHeader = false;

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

        public Builder paddingBetweenColumns(String paddingBetweenColumns) {
            this.paddingBetweenColumns = paddingBetweenColumns;
            return this;
        }

        public Builder horizontalBorderCharacter(char horizontalBorderCharacter) {
            this.horizontalBorderCharacter = horizontalBorderCharacter;
            this.hasHorizontalBorder = true;
            return this;
        }

        public Builder hasHorizontalBorder(boolean hasHorizontalBorder) {
            this.hasHorizontalBorder = hasHorizontalBorder;
            return this;
        }

        public Builder separatorRowCharacter(char separatorRowCharacter) {
            this.separatorRowCharacter = separatorRowCharacter;
            this.hasSeparatorBelowHeader = true;
            return this;
        }

        public Builder hasSeparatorBelowHeader(boolean hasSeparatorBelowHeader) {
            this.hasSeparatorBelowHeader = hasSeparatorBelowHeader;
            return this;
        }

        public TableStructure build() {
            return new TableStructure(this);
        }
    }
}
