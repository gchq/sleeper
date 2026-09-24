/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.deploy.documentation;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.IngestQueue;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.configuration.SystemTestIngestMode;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_LOGS_AFTER_DESTROY;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.instance.PartitionSplittingProperty.PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_INGESTS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_ROWS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;

public class DemoDeploymentTemplate {

    public static SystemTestProperties createInstanceProperties() {
        SystemTestProperties instanceProperties = new SystemTestProperties();
        instanceProperties.setEnum(INGEST_MODE, SystemTestIngestMode.DIRECT);
        instanceProperties.setEnum(INGEST_QUEUE, IngestQueue.STANDARD_INGEST);
        instanceProperties.setNumber(NUMBER_OF_WRITERS, 11);
        instanceProperties.setNumber(NUMBER_OF_INGESTS_PER_WRITER, 1);
        instanceProperties.setNumber(NUMBER_OF_ROWS_PER_INGEST, 40_000_000);
        instanceProperties.set(LOGGING_LEVEL, "debug");
        instanceProperties.setEnumList(OPTIONAL_STACKS, List.of(
                OptionalStack.CompactionStack,
                OptionalStack.GarbageCollectorStack,
                OptionalStack.IngestStack,
                OptionalStack.IngestBatcherStack,
                OptionalStack.PartitionSplittingStack,
                OptionalStack.QueryStack,
                OptionalStack.WebSocketQueryStack,
                OptionalStack.AthenaStack,
                OptionalStack.EmrBulkImportStack,
                OptionalStack.EmrServerlessBulkImportStack,
                OptionalStack.EmrStudioStack,
                OptionalStack.DashboardStack,
                OptionalStack.TableMetricsStack,
                OptionalStack.RestApiStack));
        instanceProperties.set(RETAIN_INFRA_AFTER_DESTROY, "false");
        instanceProperties.set(RETAIN_LOGS_AFTER_DESTROY, "true");
        instanceProperties.setNumber(PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES, 2);
        instanceProperties.setNumber(GARBAGE_COLLECTOR_PERIOD_IN_MINUTES, 2);
        instanceProperties.setTags(Map.of(
                "Description", "Sleeper demonstration instance",
                "Project", "sleeper-demo",
                "Environment", "DEV",
                "Product", "Sleeper",
                "ApplicationID", "SLEEPER"));
        return instanceProperties;
    }

    public static TableProperties createTableProperties(InstanceProperties instanceProperties) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "system-test");
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build());
        return tableProperties;
    }
}
