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

import java.util.Objects;

public class TableFieldDefinition implements TableFieldReference {
    private final String header;
    private final HorizontalAlignment horizontalAlignment;

    private TableFieldDefinition(Builder builder) {
        header = Objects.requireNonNull(builder.header, "header must not be null");
        horizontalAlignment = Objects.requireNonNull(builder.horizontalAlignment, "horizontalAlignment must not be null");
    }

    public static TableFieldDefinition numeric(String header) {
        return builder().header(header).alignRight().build();
    }

    public static TableFieldDefinition field(String header) {
        return builder().header(header).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getHeader() {
        return header;
    }

    public HorizontalAlignment getHorizontalAlignment() {
        return horizontalAlignment;
    }

    public enum HorizontalAlignment {
        LEFT, RIGHT
    }

    public static final class Builder {
        private String header;
        private HorizontalAlignment horizontalAlignment = HorizontalAlignment.LEFT;

        private Builder() {
        }

        public Builder header(String header) {
            this.header = header;
            return this;
        }

        public Builder horizontalAlignment(HorizontalAlignment horizontalAlignment) {
            this.horizontalAlignment = horizontalAlignment;
            return this;
        }

        public Builder alignLeft() {
            return horizontalAlignment(HorizontalAlignment.LEFT);
        }

        public Builder alignRight() {
            return horizontalAlignment(HorizontalAlignment.RIGHT);
        }

        public TableFieldDefinition build() {
            return new TableFieldDefinition(this);
        }
    }
}
