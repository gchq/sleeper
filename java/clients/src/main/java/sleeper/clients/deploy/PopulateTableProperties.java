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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Locale;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class PopulateTableProperties {

    private PopulateTableProperties() {
    }

    public static TableProperties from(InstanceProperties instanceProperties, Schema schema, String tableName) {
        return from(instanceProperties, new SchemaSerDe().toJson(schema), new TableProperties(instanceProperties), tableName);
    }

    public static TableProperties from(InstanceProperties instanceProperties, String schemaJson, Path tablePropertiesFile, String tableName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        try {
            tableProperties.load(tablePropertiesFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return from(instanceProperties, schemaJson, tableProperties, tableName);
    }

    public static TableProperties from(InstanceProperties instanceProperties, String schemaJson, TableProperties tableProperties, String tableName) {
        tableProperties.setSchema(new SchemaSerDe().fromJson(schemaJson));
        tableProperties.set(SCHEMA, schemaJson);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(DATA_BUCKET, String.join("-", "sleeper", instanceProperties.get(ID), "table", tableName).toLowerCase(Locale.ROOT));
        return tableProperties;
    }
}
