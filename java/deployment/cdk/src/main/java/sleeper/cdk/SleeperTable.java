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
package sleeper.cdk;

import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Properties;

import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class SleeperTable {

    private String tableName;
    private String instanceId;
    private String constructId;
    private Schema schema;

    public SleeperTable(Builder builder) {
        this.tableName = builder.tableName;
        this.instanceId = builder.instanceId;
        this.constructId = builder.constructId;
        this.schema = builder.schema;
    }

    public String saveAsString() {
        Properties properties = new Properties();
        properties.setProperty(TABLE_NAME.getPropertyName(), tableName);
        properties.setProperty(SCHEMA.getPropertyName(), new SchemaSerDe().toJson(schema));

        StringWriter stringWriter = new StringWriter();
        try {
            properties.store(stringWriter, "");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return stringWriter.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        String tableName;
        String instanceId;
        String constructId;
        Schema schema;

        private Builder() {

        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder constructId(String constructId) {
            this.instanceId = constructId;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public SleeperTable build() {
            return new SleeperTable(this);
        }

    }
}
