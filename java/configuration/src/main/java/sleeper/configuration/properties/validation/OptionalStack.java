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

import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum OptionalStack {
    CompactionStack,
    GarbageCollectorStack,
    IngestStack,
    IngestBatcherStack,
    PartitionSplittingStack,
    QueryStack,
    AthenaStack,
    EmrServerlessBulkImportStack,
    EmrStudioStack,
    DashboardStack,
    TableMetricsStack;

    public static boolean isValid(String value) {
        return EnumUtils.isValidEnumIgnoreCase(OptionalStack.class, value);
    }

    public static String getDefaultList() {
        return Stream.of(CompactionStack, GarbageCollectorStack, IngestStack, IngestBatcherStack, PartitionSplittingStack,
                QueryStack, AthenaStack, EmrServerlessBulkImportStack, EmrStudioStack, DashboardStack, TableMetricsStack)
                .map(a -> a.toString())
                .collect(Collectors.joining(","));

    }
}
