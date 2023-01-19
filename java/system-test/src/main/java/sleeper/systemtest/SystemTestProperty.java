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
            .validationPredicate(Objects::nonNull).build();
    SystemTestProperty NUMBER_OF_RECORDS_PER_WRITER = named("sleeper.systemtest.records-per-writer")
            .validationPredicate(Objects::nonNull).build();
    SystemTestProperty INGEST_MODE = named("sleeper.systemtest.ingest.mode")
            .validationPredicate(s -> EnumUtils.isValidEnumIgnoreCase(IngestMode.class, s)).build();
    SystemTestProperty SYSTEM_TEST_CLUSTER_NAME = named("sleeper.systemtest.cluster").build();
    SystemTestProperty SYSTEM_TEST_REPO = named("sleeper.systemtest.repo")
            .validationPredicate(Objects::nonNull).build();
    SystemTestProperty WRITE_DATA_TASK_DEFINITION_FAMILY = named("sleeper.systemtest.task-definition").build();
    SystemTestProperty SYSTEM_TEST_TASK_CPU = named("sleeper.systemtest.task.cpu")
            .defaultValue("1024").build();
    SystemTestProperty SYSTEM_TEST_TASK_MEMORY = named("sleeper.systemtest.task.memory")
            .defaultValue("4096").build();
    SystemTestProperty MIN_RANDOM_INT = named("sleeper.systemtest.random.int.min")
            .defaultValue("0").build();
    SystemTestProperty MAX_RANDOM_INT = named("sleeper.systemtest.random.int.max")
            .defaultValue("100000000").build();
    SystemTestProperty MIN_RANDOM_LONG = named("sleeper.systemtest.random.long.min")
            .defaultValue("0").build();
    SystemTestProperty MAX_RANDOM_LONG = named("sleeper.systemtest.random.long.max")
            .defaultValue("10000000000").build();
    SystemTestProperty RANDOM_STRING_LENGTH = named("sleeper.systemtest.random.string.length")
            .defaultValue("10").build();
    SystemTestProperty RANDOM_BYTE_ARRAY_LENGTH = named("sleeper.systemtest.random.bytearray.length")
            .defaultValue("10").build();
    SystemTestProperty MAX_ENTRIES_RANDOM_MAP = named("sleeper.systemtest.random.map.length")
            .defaultValue("10").build();
    SystemTestProperty MAX_ENTRIES_RANDOM_LIST = named("sleeper.systemtest.random.list.length")
            .defaultValue("10").build();

    static SystemTestProperty[] values() {
        return SystemTestPropertyImpl.all().toArray(new SystemTestProperty[0]);
    }
}
