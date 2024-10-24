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
package sleeper.systemtest.drivers.testutil;

import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;

import java.util.function.Consumer;

import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration.usingSystemTestDefaults;

public class LocalStackTestInstance {

    private LocalStackTestInstance() {
    }

    public static final String ROW_KEY_FIELD_NAME = "key";
    public static final String SORT_KEY_FIELD_NAME = "timestamp";
    public static final String VALUE_FIELD_NAME = "value";
    public static final Schema DEFAULT_SCHEMA = Schema.builder()
            .rowKeyFields(new Field(ROW_KEY_FIELD_NAME, new StringType()))
            .sortKeyFields(new Field(SORT_KEY_FIELD_NAME, new LongType()))
            .valueFields(new Field(VALUE_FIELD_NAME, new StringType()))
            .build();

    public static final SystemTestInstanceConfiguration MAIN = usingSystemTestDefaults("main", LocalStackTestInstance::buildMainConfiguration);
    public static final SystemTestInstanceConfiguration PREDEFINED_TABLE = usingSystemTestDefaults("prdftbl", LocalStackTestInstance::buildPredefinedTableConfiguration);
    public static final SystemTestInstanceConfiguration PREDEFINED_TABLE_NO_NAME = usingSystemTestDefaults("prdftnn", LocalStackTestInstance::buildPredefinedTableConfigurationNoName);

    private static DeployInstanceConfiguration buildMainConfiguration() {
        return buildConfigurationWithTableProperties(properties -> properties.set(TABLE_NAME, "system-test"));
    }

    private static DeployInstanceConfiguration buildPredefinedTableConfiguration() {
        return buildConfigurationWithTableProperties(properties -> properties.set(TABLE_NAME, "predefined-test-table"));
    }

    private static DeployInstanceConfiguration buildPredefinedTableConfigurationNoName() {
        return buildConfigurationWithTableProperties(properties -> properties.unset(TABLE_NAME));
    }

    private static DeployInstanceConfiguration buildConfigurationWithTableProperties(Consumer<TableProperties> setTableProperties) {
        InstanceProperties properties = new InstanceProperties();
        properties.set(FORCE_RELOAD_PROPERTIES, "true");
        properties.set(DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS, "true");
        properties.set(RETAIN_INFRA_AFTER_DESTROY, "false");

        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.setSchema(DEFAULT_SCHEMA);
        setTableProperties.accept(tableProperties);

        return DeployInstanceConfiguration.builder()
                .instanceProperties(properties)
                .tableProperties(tableProperties)
                .build();
    }

}
