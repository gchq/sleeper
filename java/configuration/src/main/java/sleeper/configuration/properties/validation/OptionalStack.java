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
package sleeper.configuration.properties.validation;

import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.SleeperPropertyValues;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum OptionalStack {

    // Ingest
    IngestStack,
    IngestBatcherStack,

    // Bulk import
    EmrBulkImportStack,
    EmrServerlessBulkImportStack,
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

    public static boolean isValid(String value) {
        return SleeperPropertyValues.readList(value).stream()
                .allMatch(item -> EnumUtils.isValidEnumIgnoreCase(OptionalStack.class, item));
    }

    public static String getDefaultValue() {
        return Stream.of(CompactionStack, GarbageCollectorStack, IngestStack, IngestBatcherStack, PartitionSplittingStack,
                QueryStack, AthenaStack, EmrServerlessBulkImportStack, EmrStudioStack, DashboardStack, TableMetricsStack)
                .map(a -> a.toString())
                .collect(Collectors.joining(","));
    }
}
