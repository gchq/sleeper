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
package sleeper.systemtest.drivers.testutil;

import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;

import java.util.function.Consumer;

import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration.usingSystemTestDefaults;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

public class LocalStackTestInstance {

    private LocalStackTestInstance() {
    }

    public static final SystemTestInstanceConfiguration LOCALSTACK_MAIN = usingSystemTestDefaults("main", LocalStackTestInstance::buildMainConfiguration);
    public static final SystemTestInstanceConfiguration DRAIN_COMPACTIONS = usingSystemTestDefaults("cpt-dr", LocalStackTestInstance::buildMainConfiguration);
    public static final SystemTestInstanceConfiguration PREDEFINED_TABLE = usingSystemTestDefaults("prdtbl", LocalStackTestInstance::buildPredefinedTableConfiguration);
    public static final SystemTestInstanceConfiguration PREDEFINED_TABLE_NO_NAME = usingSystemTestDefaults("prdtnn", LocalStackTestInstance::buildPredefinedTableConfigurationNoName);

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
