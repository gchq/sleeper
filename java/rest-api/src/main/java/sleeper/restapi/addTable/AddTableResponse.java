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

import java.util.Objects;

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
     * Checks that the object created is valid and has all of the required fields.
     *
     * @return the validated object
     */
    public AddTableResponse validate() {
        return AddTableResponse.builder()
                .tableId(tableId)
                .tableName(tableName)
                .build();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        AddTableResponse that = (AddTableResponse) object;
        return Objects.equals(tableId, that.tableId) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tableName);
    }

    @Override
    public String toString() {
        return "tableId: " + tableId + ", tableName: " + tableName;
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
         * @param  tableId the table id
         * @return         the builder for chaining
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the table name.
         *
         * @param  tableName the table name
         * @return           the builder for chaining
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
