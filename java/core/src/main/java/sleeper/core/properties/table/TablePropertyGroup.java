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

package sleeper.core.properties.table;

import sleeper.core.properties.PropertyGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static sleeper.core.properties.PropertyGroup.group;

/**
 * Definitions of groups used to organise table properties.
 */
public class TablePropertyGroup {
    private TablePropertyGroup() {
    }

    private static final List<PropertyGroup> ALL = new ArrayList<>();
    public static final PropertyGroup DATA_DEFINITION = tableGroup("Data Definition")
            .description("The following table properties relate to the definition of data inside a table.").build();
    public static final PropertyGroup PARTITION_SPLITTING = tableGroup("Partition Splitting")
            .description("The following table properties relate to partition splitting.").build();
    public static final PropertyGroup DATA_STORAGE = tableGroup("Data Storage")
            .description("The following table properties relate to the storage of data inside a table.").build();
    public static final PropertyGroup COMPACTION = tableGroup("Compaction")
            .description("The following table properties relate to compactions.").build();
    public static final PropertyGroup METADATA = tableGroup("Metadata")
            .description("The following table properties relate to storing and retrieving metadata for tables.").build();
    public static final PropertyGroup INGEST = tableGroup("Ingest")
            .description("The following table properties relate to ingest.").build();
    public static final PropertyGroup BULK_IMPORT = tableGroup("Bulk Import")
            .description("The following table properties relate to bulk import, " +
                    "i.e. ingesting data using Spark jobs running on EMR or EKS.")
            .build();
    public static final PropertyGroup INGEST_BATCHER = tableGroup("Ingest Batcher")
            .description("The following table properties relate to the ingest batcher.").build();
    public static final PropertyGroup QUERY_EXECUTION = tableGroup("Query Execution")
            .description("The following table properties relate to query execution").build();

    private static PropertyGroup.Builder tableGroup(String name) {
        return group(name).afterBuild(ALL::add);
    }

    public static List<PropertyGroup> getAll() {
        return Collections.unmodifiableList(ALL);
    }

}
