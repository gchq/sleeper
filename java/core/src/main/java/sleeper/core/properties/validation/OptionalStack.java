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
package sleeper.core.properties.validation;

import org.apache.commons.lang3.EnumUtils;

import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * Valid values for optional deployment stacks. Determines which components of Sleeper will be deployed.
 */
public enum OptionalStack {

    // Ingest
    IngestStack,
    IngestBatcherStack,

    // Bulk import
    EmrServerlessBulkImportStack,
    EmrBulkImportStack,
    PersistentEmrBulkImportStack,
    EksBulkImportStack,
    EmrStudioStack,

    // Query
    QueryStack,
    WebSocketQueryStack,
    AthenaStack,
    KeepLambdaWarmStack,

    // Data maintenance
    CompactionStack,
    GarbageCollectorStack,
    PartitionSplittingStack,

    // Metrics
    DashboardStack,
    TableMetricsStack;

    public static final List<OptionalStack> BULK_IMPORT_STACKS = List.of(
            EmrBulkImportStack,
            EmrServerlessBulkImportStack,
            PersistentEmrBulkImportStack,
            EksBulkImportStack);

    public static final List<OptionalStack> EMR_BULK_IMPORT_STACKS = List.of(
            EmrBulkImportStack,
            EmrServerlessBulkImportStack,
            PersistentEmrBulkImportStack);

    public static final List<OptionalStack> INGEST_STACKS = List.of(
            IngestStack,
            EmrBulkImportStack,
            EmrServerlessBulkImportStack,
            PersistentEmrBulkImportStack,
            EksBulkImportStack);

    public static final List<OptionalStack> QUERY_STACKS = List.of(
            QueryStack,
            WebSocketQueryStack);

    public static final List<OptionalStack> SYSTEM_TEST_STACKS = List.of(
            IngestStack,
            EmrBulkImportStack,
            EmrServerlessBulkImportStack,
            IngestBatcherStack,
            CompactionStack,
            GarbageCollectorStack,
            PartitionSplittingStack,
            QueryStack,
            WebSocketQueryStack,
            TableMetricsStack,
            AthenaStack);

    public static final List<OptionalStack> LOCALSTACK_STACKS = List.of(
            IngestStack,
            CompactionStack,
            PartitionSplittingStack,
            QueryStack);

    public static final List<OptionalStack> DEFAULT_STACKS = List.of(
            IngestStack, IngestBatcherStack, EmrServerlessBulkImportStack, EmrStudioStack,
            QueryStack, AthenaStack, CompactionStack, GarbageCollectorStack, PartitionSplittingStack,
            DashboardStack, TableMetricsStack);

    /**
     * Checks if the value is a valid optional deployment stack.
     *
     * @param  value the value
     * @return       true if it is valid
     */
    public static boolean isValid(String value) {
        return SleeperPropertyValueUtils.readList(value).stream()
                .allMatch(item -> EnumUtils.isValidEnumIgnoreCase(OptionalStack.class, item));
    }

    /**
     * Returns the default value for the property to set optional stacks for an instance. This value is a
     * comma-separated string.
     *
     * @return the default value
     */
    public static String getDefaultValue() {
        return DEFAULT_STACKS.stream()
                .map(OptionalStack::toString)
                .collect(joining(","));
    }

    /**
     * Returns a list of all optional stacks.
     *
     * @return all optional stacks
     */
    public static List<OptionalStack> all() {
        return List.of(values());
    }
}
