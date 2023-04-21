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

import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.schema.Schema;

import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.configuration.properties.table.TableProperty.FILE_IN_PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.FILE_LIFECYCLE_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesTestHelper {

    private TablePropertiesTestHelper() {
    }

    public static TableProperties createTestTableProperties(
            InstanceProperties instanceProperties, Schema schema, AmazonS3 s3) {
        return createTestTableProperties(instanceProperties, schema, s3, properties -> {
        });
    }

    public static TableProperties createTestTableProperties(
            InstanceProperties instanceProperties, Schema schema, AmazonS3 s3, Consumer<TableProperties> tableConfig) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableConfig.accept(tableProperties);
        save(tableProperties, s3);
        return tableProperties;
    }

    private static void save(TableProperties tableProperties, AmazonS3 s3) {
        try {
            tableProperties.saveToS3(s3);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save table properties", e);
        }
    }

    public static TableProperties createTestTableProperties(InstanceProperties instanceProperties, Schema schema) {
        TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    public static TableProperties createTestTablePropertiesWithNoSchema(InstanceProperties instanceProperties) {
        String tableName = UUID.randomUUID().toString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(DATA_BUCKET, tableName + "-data");
        tableProperties.set(FILE_IN_PARTITION_TABLENAME, tableName + "-fip");
        tableProperties.set(FILE_LIFECYCLE_TABLENAME, tableName + "-fl");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        return tableProperties;
    }
}
