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

package sleeper.systemtest.dsl.testutil;

import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;

import java.util.function.Consumer;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration.usingSystemTestDefaults;

public class InMemoryTestInstance {
    private InMemoryTestInstance() {
    }

    public static final String ROW_KEY_FIELD_NAME = "key";
    public static final String SORT_KEY_FIELD_NAME = "timestamp";
    public static final String VALUE_FIELD_NAME = "value";
    public static final Schema DEFAULT_SCHEMA = Schema.builder()
            .rowKeyFields(new Field(ROW_KEY_FIELD_NAME, new StringType()))
            .sortKeyFields(new Field(SORT_KEY_FIELD_NAME, new LongType()))
            .valueFields(new Field(VALUE_FIELD_NAME, new StringType()))
            .build();
    public static final SystemTestInstanceConfiguration MAIN = withDefaultProperties("main");
    public static final SystemTestInstanceConfiguration PREDEFINED_TABLE = usingSystemTestDefaults("prdtbl", () -> {
        InstanceProperties instanceProperties = createDslInstanceProperties();
        TableProperties tableProperties = createDslTableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "predefined-test-table");
        return new DeployInstanceConfiguration(instanceProperties, tableProperties);
    });
    public static final SystemTestInstanceConfiguration PREDEFINED_TABLE_NO_NAME = usingSystemTestDefaults("nontbl", () -> {
        InstanceProperties instanceProperties = createDslInstanceProperties();
        TableProperties tableProperties = createDslTableProperties(instanceProperties);
        tableProperties.unset(TABLE_NAME);
        return new DeployInstanceConfiguration(instanceProperties, tableProperties);
    });

    public static SystemTestInstanceConfiguration withDefaultProperties(String identifier) {
        return withInstanceProperties(identifier, properties -> {
        });
    }

    public static SystemTestInstanceConfiguration withInstanceProperties(
            String identifier, Consumer<InstanceProperties> config) {
        return usingSystemTestDefaults(identifier, () -> {
            InstanceProperties instanceProperties = createDslInstanceProperties();
            config.accept(instanceProperties);
            return DeployInstanceConfiguration.builder()
                    .instanceProperties(instanceProperties)
                    .tableProperties(createDslTableProperties(instanceProperties))
                    .build();
        });
    }

    public static InstanceProperties createDslInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(RETAIN_INFRA_AFTER_DESTROY, "false");
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "in-memory-ingest-job-queue-url");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, "in-memory-serverless-bulk-import-job-queue-url");
        return instanceProperties;
    }

    public static TableProperties createDslTableProperties(InstanceProperties instanceProperties) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, DEFAULT_SCHEMA);
        tableProperties.unset(TABLE_ID);
        tableProperties.set(TABLE_NAME, "system-test");
        return tableProperties;
    }
}
