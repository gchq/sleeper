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

package sleeper.configuration.properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

import static sleeper.configuration.properties.PropertyGroupImpl.group;

// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface PropertyGroup {
    PropertyGroup COMMON = group("Common")
            .description("The following properties are commonly used throughout Sleeper")
            .build();
    PropertyGroup INGEST = group("Ingest")
            .description("The following properties relate to standard ingest")
            .build();
    PropertyGroup BULK_IMPORT = group("Bulk Import")
            .description("The following properties relate to bulk import, " +
                    "i.e. ingesting data using Spark jobs running on EMR or EKS.")
            .build();
    PropertyGroup PARTITION_SPLITTING = group("Partition Splitting")
            .description("The following properties relate to the splitting of partitions")
            .build();
    PropertyGroup GARBAGE_COLLECTOR = group("Garbage Collector")
            .description("The following properties relate to garbage collection.")
            .build();
    PropertyGroup COMPACTION = group("Compaction")
            .description("The following properties relate to compactions.")
            .build();
    PropertyGroup QUERY = group("Query")
            .description("The following properties relate to queries.")
            .build();
    PropertyGroup LOGGING = group("Logging")
            .description("The following properties relate to logging.")
            .build();
    PropertyGroup ATHENA = group("Athena")
            .description("The following properties relate to the integration with Athena.")
            .build();
    PropertyGroup DEFAULT = group("Default")
            .description("The following properties relate to default values.")
            .build();

    static List<PropertyGroup> all() {
        return PropertyGroupImpl.all();
    }

    String getName();

    String getDescription();
}
