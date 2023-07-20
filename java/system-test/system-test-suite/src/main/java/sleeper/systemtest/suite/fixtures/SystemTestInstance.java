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
import sleeper.systemtest.drivers.instance.SystemTestParameters;

import java.util.function.Function;

import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;

public enum SystemTestInstance {

    MAIN("main", SystemTestInstance::buildDefaultConfiguration);

    private final String identifier;
    private final Function<SystemTestParameters, DeployInstanceConfiguration> instanceConfiguration;

    SystemTestInstance(String identifier, Function<SystemTestParameters, DeployInstanceConfiguration> instanceConfiguration) {
        this.identifier = identifier;
        this.instanceConfiguration = instanceConfiguration;
    }

    public String getIdentifier() {
        return identifier;
    }

    public DeployInstanceConfiguration getInstanceConfiguration(SystemTestParameters parameters) {
        return instanceConfiguration.apply(parameters);
    }

    private static DeployInstanceConfiguration buildDefaultConfiguration(SystemTestParameters parameters) {
        InstanceProperties properties = new InstanceProperties();
        properties.set(LOGGING_LEVEL, "debug");
        properties.set(OPTIONAL_STACKS, "IngestStack,EmrBulkImportStack,IngestBatcherStack," +
                "CompactionStack,GarbageCollectorStack,PartitionSplittingStack,QueryStack");
        properties.set(RETAIN_INFRA_AFTER_DESTROY, "false");
        properties.set(INGEST_SOURCE_BUCKET, parameters.buildSourceBucketName());

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
