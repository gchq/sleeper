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

package sleeper.configuration.properties.table;

import sleeper.configuration.properties.PropertyGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static sleeper.configuration.properties.PropertyGroup.group;

public class TablePropertyGroup {
    private TablePropertyGroup() {
    }

    private static final List<PropertyGroup> ALL = new ArrayList<>();
    public static final PropertyGroup CONFIGURATION = tableGroup("Configuration")
            .description("The following properties relate to configuring tables.").build();
    public static final PropertyGroup ITERATOR = tableGroup("Iterator")
            .description("The following table properties relate to the iterator used when reading from the table.").build();
    public static final PropertyGroup SPLIT_POINTS = tableGroup("Split Points")
            .description("The following table properties relate to the split points in the table.").build();
    public static final PropertyGroup COMPACTION = tableGroup("Compaction")
            .description("The following table properties relate to compactions.").build();
    public static final PropertyGroup PARTITION_SPLITTING = tableGroup("Partition Splitting")
            .description("The following table properties relate to partition splitting.").build();
    public static final PropertyGroup BULK_IMPORT = tableGroup("Bulk Import")
            .description("The following table properties relate to bulk import, " +
                    "i.e. ingesting data using Spark jobs running on EMR or EKS.").build();
    public static final PropertyGroup METADATA = tableGroup("Metadata")
            .description("The following table properties relate to storing and retrieving metadata for tables.").build();

    private static PropertyGroup.Builder tableGroup(String name) {
        return group(name).afterBuild(ALL::add);
    }

    public static List<PropertyGroup> getAll() {
        return Collections.unmodifiableList(ALL);
    }
}
