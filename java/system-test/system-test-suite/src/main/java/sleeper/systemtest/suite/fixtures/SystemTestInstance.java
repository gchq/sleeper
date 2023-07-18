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

package sleeper.systemtest.suite.fixtures;

import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;

public enum SystemTestInstance {

    MAIN("main", buildDefaultConfiguration());

    private final String identifier;
    private final DeployInstanceConfiguration instanceConfiguration;

    SystemTestInstance(String identifier, DeployInstanceConfiguration instanceConfiguration) {
        this.identifier = identifier;
        this.instanceConfiguration = instanceConfiguration;
    }

    public String getIdentifier() {
        return identifier;
    }

    public DeployInstanceConfiguration getInstanceConfiguration() {
        return instanceConfiguration;
    }

    private static DeployInstanceConfiguration buildDefaultConfiguration() {
        InstanceProperties properties = new InstanceProperties();
        properties.set(LOGGING_LEVEL, "debug");
        properties.set(OPTIONAL_STACKS, "IngestStack,EmrBulkImportStack,IngestBatcherStack," +
                "CompactionStack,GarbageCollectorStack,PartitionSplittingStack,QueryStack");
        properties.set(RETAIN_INFRA_AFTER_DESTROY, "false");

        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build());

        return DeployInstanceConfiguration.builder()
                .instanceProperties(properties)
                .tableProperties(tableProperties)
                .build();
    }
}
