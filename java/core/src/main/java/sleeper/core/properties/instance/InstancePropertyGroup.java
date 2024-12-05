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

package sleeper.core.properties.instance;

import sleeper.core.properties.PropertyGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static sleeper.core.properties.PropertyGroup.group;

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
    public static final PropertyGroup TABLE_STATE = instanceGroup("Table State")
            .description("The following properties relate to handling the state of Sleeper tables.")
            .build();
    public static final PropertyGroup INGEST = instanceGroup("Ingest")
            .description("The following properties relate to standard ingest.")
            .build();
    public static final PropertyGroup BULK_IMPORT = instanceGroup("Bulk Import")
            .description("The following properties relate to bulk import, " +
                    "i.e. ingesting data using Spark jobs running on EMR or EKS.")
            .details("Note that on EMR, the total resource allocation must align with the instance types used for the " +
                    "cluster. For the maximum memory usage, combine the memory and memory overhead properties, and " +
                    "compare against the maximum memory allocation for YARN in the Hadoop task configuration:\n" +
                    "\n" +
                    "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html\n" +
                    "\n" +
                    "As an example, if we use m7i.xlarge for executor instances, that has a maximum allocation of " +
                    "54272 MiB, or 53 GiB. If we want 3 executors per instance, we can have 53 GiB / 3 = 18,090.666 " +
                    "MiB per executor. We can set the executor memory to 16 GiB, and the executor memory overhead to " +
                    "the remainder of that amount, which is 18,090 MiB - 16 GiB = 1,706 MiB, or 1.666 GiB. This is " +
                    "just above the default Spark memory overhead factor of 0.1, i.e. 16 GiB x 0.1 = 1.6 GiB.\n" +
                    "\n" +
                    "Also see EMR best practices:\n" +
                    "\n" +
                    "https://aws.github.io/aws-emr-best-practices/docs/bestpractices/Applications/Spark/best_practices/#bp-516----tune-driverexecutor-memory-cores-and-sparksqlshufflepartitions-to-fully-utilize-cluster-resources")
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
    public static final PropertyGroup METRICS = instanceGroup("Metrics")
            .description("The following properties relate to metrics.")
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
