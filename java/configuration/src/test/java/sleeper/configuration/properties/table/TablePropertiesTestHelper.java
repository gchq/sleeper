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
package sleeper.configuration.properties.table;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIdGenerator;

import java.util.UUID;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesTestHelper {

    private TablePropertiesTestHelper() {
    }

    private static final TableIdGenerator TABLE_ID_GENERATOR = new TableIdGenerator();

    public static TableProperties createTestTableProperties(InstanceProperties instanceProperties, Schema schema) {
        TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    public static TableProperties createTestTablePropertiesWithNoSchema(InstanceProperties instanceProperties) {
        String tableName = UUID.randomUUID().toString();
        String tableId = TABLE_ID_GENERATOR.generateString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(TABLE_ID, tableId);
        return tableProperties;
    }
}
