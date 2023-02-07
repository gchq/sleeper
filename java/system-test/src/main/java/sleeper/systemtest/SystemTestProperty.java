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
package sleeper.systemtest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.InstanceProperty;
import sleeper.systemtest.ingest.IngestMode;

import java.util.Objects;

import static sleeper.systemtest.SystemTestPropertyImpl.named;

// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface SystemTestProperty extends InstanceProperty {
    SystemTestProperty NUMBER_OF_WRITERS = named("sleeper.systemtest.writers")
            .description("The number of containers that write random data")
            .validationPredicate(Objects::nonNull).build();
    SystemTestProperty NUMBER_OF_RECORDS_PER_WRITER = named("sleeper.systemtest.records-per-writer")
            .description("The number of random records that each container should write")
            .validationPredicate(Objects::nonNull).build();
    SystemTestProperty INGEST_MODE = named("sleeper.systemtest.ingest.mode")
            .description("The ingest mode to write random data. This should be either 'direct' or 'queue'.\n" +
                    "'Direct' means that the data is written directly.\n" +
                    "'Queue' means that the data is written to a Parquet file and an ingest job is created " +
                    "and posted to the ingest queue.")
            .validationPredicate(s -> EnumUtils.isValidEnumIgnoreCase(IngestMode.class, s)).build();
    SystemTestProperty SYSTEM_TEST_CLUSTER_NAME = named("sleeper.systemtest.cluster")
            .description("The name of the cluster to use when performing system tests").build();
    SystemTestProperty SYSTEM_TEST_REPO = named("sleeper.systemtest.repo")
            .description("The image in ECR used for writing random data to the system")
            .validationPredicate(Objects::nonNull).build();
    SystemTestProperty WRITE_DATA_TASK_DEFINITION_FAMILY = named("sleeper.systemtest.task-definition")
            .description("The name of the family of task definitions used for writing data").build();
    SystemTestProperty SYSTEM_TEST_TASK_CPU = named("sleeper.systemtest.task.cpu")
            .description("The amount of CPU for the containers that write random data")
            .defaultValue("1024").build();
    SystemTestProperty SYSTEM_TEST_TASK_MEMORY = named("sleeper.systemtest.task.memory")
            .description("The amount of memory for the containers that write random data")
            .defaultValue("4096").build();
    SystemTestProperty MIN_RANDOM_INT = named("sleeper.systemtest.random.int.min")
            .description("The minimum value of integers generated randomly during random record generation")
            .defaultValue("0").build();
    SystemTestProperty MAX_RANDOM_INT = named("sleeper.systemtest.random.int.max")
            .description("The maximum value of integers generated randomly during random record generation")
            .defaultValue("100000000").build();
    SystemTestProperty MIN_RANDOM_LONG = named("sleeper.systemtest.random.long.min")
            .description("The minimum value of longs generated randomly during random record generation")
            .defaultValue("0").build();
    SystemTestProperty MAX_RANDOM_LONG = named("sleeper.systemtest.random.long.max")
            .description("The maximum value of longs generated randomly during random record generation")
            .defaultValue("10000000000").build();
    SystemTestProperty RANDOM_STRING_LENGTH = named("sleeper.systemtest.random.string.length")
            .description("The length of strings generated randomly during random record generation")
            .defaultValue("10").build();
    SystemTestProperty RANDOM_BYTE_ARRAY_LENGTH = named("sleeper.systemtest.random.bytearray.length")
            .description("The length of byte arrays generated randomly during random record generation")
            .defaultValue("10").build();
    SystemTestProperty MAX_ENTRIES_RANDOM_MAP = named("sleeper.systemtest.random.map.length")
            .description("The maximum number of entries in maps generated randomly during random record generation\n" +
                    "(the number of entries in the map will range randomly from 0 to this number)")
            .defaultValue("10").build();
    SystemTestProperty MAX_ENTRIES_RANDOM_LIST = named("sleeper.systemtest.random.list.length")
            .description("The maximum number of entries in lists generated randomly during random record generation\n" +
                    "(the number of entries in the list will range randomly from 0 to this number)")
            .defaultValue("10").build();

    static SystemTestProperty[] values() {
        return SystemTestPropertyImpl.all().toArray(new SystemTestProperty[0]);
    }

    static SystemTestProperty fromName(String propertyName) {
        return SystemTestPropertyImpl.get(propertyName);
    }
}
