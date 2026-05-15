/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.restapi.addTable;

/**
 * Decoded JSON body for the response back out from /sleeper/tables.
 */
public class AddTableResponse {
    private String tableId;
    private String tableName;

    private AddTableResponse(Builder builder) {
        tableId = builder.tableId;
        tableName = builder.tableName;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to create an AddTable response.
     */
    public static final class Builder {
        private String tableId;
        private String tableName;

        private Builder() {
        }

        /**
         * Sets the table id.
         *
         * @param  properties the table id
         * @return            the builder for chaining
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the table name.
         *
         * @param  properties the table name
         * @return            the builder for chaining
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public AddTableResponse build() {
            return new AddTableResponse(this);
        }
    }
}
