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

import java.util.List;

public class PropertyGroup {
    public static final PropertyGroup COMMON = group("The following properties are commonly used throughout Sleeper")
            .build();
    public static final PropertyGroup INGEST = group("The following properties relate to standard ingest")
            .build();
    public static final PropertyGroup BULK_IMPORT = group("The following properties relate to bulk import, " +
            "i.e. ingesting data using Spark jobs running on EMR or EKS.")
            .build();
    public static final PropertyGroup PARTITION_SPLITTING = group("The following properties relate to the splitting of partitions")
            .build();
    public static final PropertyGroup GARBAGE_COLLECTOR = group("The following properties relate to garbage collection.")
            .build();
    public static final PropertyGroup COMPACTION = group("The following properties relate to compactions.")
            .build();
    public static final PropertyGroup QUERY = group("The following properties relate to queries.")
            .build();
    public static final PropertyGroup LOGGING = group("The following properties relate to logging.")
            .build();
    public static final PropertyGroup DEFAULT = group("The following properties relate to default values.")
            .build();
    public static final PropertyGroup ATHENA = group("The following properties relate to the integration with Athena.")
            .build();
    public static final PropertyGroup UNKNOWN = group("The following properties have no defined property grouping")
            .build();
    public static final List<PropertyGroup> ALL = List.of(COMMON, INGEST, BULK_IMPORT, PARTITION_SPLITTING, GARBAGE_COLLECTOR,
            COMPACTION, QUERY, LOGGING, DEFAULT, ATHENA, UNKNOWN);

    private final String description;

    private PropertyGroup(Builder builder) {
        description = builder.description;
    }

    private static Builder group(String description) {
        return builder().description(description);
    }

    private static Builder builder() {
        return new Builder();
    }

    public String getDescription() {
        return description;
    }

    public static List<PropertyGroup> all() {
        return ALL;
    }

    private static final class Builder {
        private String description;

        private Builder() {
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public PropertyGroup build() {
            return new PropertyGroup(this);
        }
    }
}
