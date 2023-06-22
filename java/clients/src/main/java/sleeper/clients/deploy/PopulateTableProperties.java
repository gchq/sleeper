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
package sleeper.clients.deploy;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.util.Locale;
import java.util.Objects;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class PopulateTableProperties {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final String schemaJson;
    private final String tableName;


    private PopulateTableProperties(Builder builder) {
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        tableProperties = Objects.requireNonNullElse(builder.tableProperties, new TableProperties(instanceProperties));
        schemaJson = builder.schemaJson;
        tableName = builder.tableName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public TableProperties populate() {
        if (schemaJson != null) {
            tableProperties.setSchema(new SchemaSerDe().fromJson(schemaJson));
            tableProperties.set(SCHEMA, schemaJson);
        }
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(DATA_BUCKET, String.join("-", "sleeper", instanceProperties.get(ID), "table", tableName).toLowerCase(Locale.ROOT));
        return tableProperties;
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties;
        private String schemaJson;
        private String tableName;

        public Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schemaJson = new SchemaSerDe().toJson(schema);
            return this;
        }

        public Builder schema(String schemaJson) {
            this.schemaJson = schemaJson;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public PopulateTableProperties build() {
            return new PopulateTableProperties(this);
        }
    }
}
