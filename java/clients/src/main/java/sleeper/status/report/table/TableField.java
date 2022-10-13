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

public class TableField {

    private final int index;
    private final String header;
    private final HorizontalAlignment horizontalAlignment;

    private TableField(Builder builder) {
        index = builder.index;
        header = Objects.requireNonNull(builder.header, "header must not be null");
        horizontalAlignment = Objects.requireNonNull(builder.horizontalAlignment, "horizontalAlignment must not be null");
    }

    static Builder builder(TableWriterFactory.Builder table, int index) {
        return new Builder(table, index);
    }

    public String getHeader() {
        return header;
    }

    public int getIndex() {
        return index;
    }

    public HorizontalAlignment getHorizontalAlignment() {
        return horizontalAlignment;
    }

    public enum HorizontalAlignment {
        LEFT, RIGHT
    }

    public static final class Builder {
        private final TableWriterFactory.Builder table;
        private final int index;
        private String header;
        private HorizontalAlignment horizontalAlignment;

        private Builder(TableWriterFactory.Builder table, int index) {
            this.table = table;
            this.index = index;
        }

        public Builder header(String header) {
            this.header = header;
            return this;
        }

        public Builder alignLeft() {
            return horizontalAlignment(HorizontalAlignment.LEFT);
        }

        public Builder alignRight() {
            return horizontalAlignment(HorizontalAlignment.RIGHT);
        }

        private Builder horizontalAlignment(HorizontalAlignment horizontalAlignment) {
            this.horizontalAlignment = horizontalAlignment;
            return this;
        }

        public TableField build() {
            return table.addField(new TableField(this));
        }
    }
}
