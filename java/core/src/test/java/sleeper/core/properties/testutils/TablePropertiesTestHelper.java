/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.core.properties.testutils;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIdGenerator;

import java.util.UUID;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Helpers to create table properties.
 */
public class TablePropertiesTestHelper {

    private TablePropertiesTestHelper() {
    }

    private static final TableIdGenerator TABLE_ID_GENERATOR = new TableIdGenerator();

    /**
     * Creates properties for a Sleeper table with the given schema. Generates a random table name and ID.
     *
     * @param  instanceProperties the instance properties
     * @param  schema             the schema
     * @return                    the table properties
     */
    public static TableProperties createTestTableProperties(InstanceProperties instanceProperties, Schema schema) {
        TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    /**
     * Creates properties for a Sleeper table with no schema. Generates a random table name and ID. Usually used for
     * tests where the schema will be set later on.
     *
     * @param  instanceProperties the instance properties
     * @return                    the table properties
     */
    public static TableProperties createTestTablePropertiesWithNoSchema(InstanceProperties instanceProperties) {
        String tableName = UUID.randomUUID().toString();
        String tableId = TABLE_ID_GENERATOR.generateString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(TABLE_ID, tableId);
        return tableProperties;
    }
}
