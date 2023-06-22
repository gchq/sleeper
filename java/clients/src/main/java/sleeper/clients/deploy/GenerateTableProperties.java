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
import java.util.Properties;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class GenerateTableProperties {

    private GenerateTableProperties() {
    }

    public static TableProperties from(InstanceProperties instanceProperties, Schema schema, String tableName) {
        return from(instanceProperties, new SchemaSerDe().toJson(schema), new Properties(), tableName);
    }

    public static TableProperties from(InstanceProperties instanceProperties, String schemaJson,
                                       Properties properties, String tableName) {
        properties.setProperty(SCHEMA.getPropertyName(), schemaJson);
        properties.setProperty(TABLE_NAME.getPropertyName(), tableName);
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        tableProperties.set(DATA_BUCKET, String.join("-", "sleeper", instanceProperties.get(ID), "table", tableName).toLowerCase(Locale.ROOT));
        return tableProperties;
    }
}
