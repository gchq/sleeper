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

package sleeper.configuration.properties.instance;

import sleeper.configuration.properties.PropertyGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static sleeper.configuration.properties.PropertyGroup.group;

/**
 * Definitions of groups used to organise instance properties.
 */
public class InstancePropertyGroup {
    private InstancePropertyGroup() {
    }

    private static final List<PropertyGroup> ALL = new ArrayList<>();
    public static final PropertyGroup COMMON = instanceGroup("Common")
            .description("The following properties are commonly used throughout Sleeper.")
            .build();
    public static final PropertyGroup INGEST = instanceGroup("Ingest")
            .description("The following properties relate to standard ingest.")
            .build();
    public static final PropertyGroup BULK_IMPORT = instanceGroup("Bulk Import")
            .description("The following properties relate to bulk import, " +
                    "i.e. ingesting data using Spark jobs running on EMR or EKS.")
            .build();
    public static final PropertyGroup PARTITION_SPLITTING = instanceGroup("Partition Splitting")
            .description("The following properties relate to the splitting of partitions.")
            .build();
    public static final PropertyGroup GARBAGE_COLLECTOR = instanceGroup("Garbage Collector")
            .description("The following properties relate to garbage collection.")
            .build();
    public static final PropertyGroup COMPACTION = instanceGroup("Compaction")
            .description("The following properties relate to compactions.")
            .build();
    public static final PropertyGroup QUERY = instanceGroup("Query")
            .description("The following properties relate to queries.")
            .build();
    public static final PropertyGroup DASHBOARD = instanceGroup("Dashboard")
            .description("The following properties relate to the dashboard.")
            .build();
    public static final PropertyGroup LOGGING = instanceGroup("Logging")
            .description("The following properties relate to logging.")
            .build();
    public static final PropertyGroup ATHENA = instanceGroup("Athena")
            .description("The following properties relate to the integration with Athena.")
            .build();
    public static final PropertyGroup DEFAULT = instanceGroup("Default")
            .description("The following properties relate to default values used by table properties.")
            .build();

    private static PropertyGroup.Builder instanceGroup(String name) {
        return group(name).afterBuild(ALL::add);
    }

    public static List<PropertyGroup> getAll() {
        return Collections.unmodifiableList(ALL);
    }
}
