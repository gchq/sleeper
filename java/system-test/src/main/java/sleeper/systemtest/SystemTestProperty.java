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

import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.InstanceProperty;
import sleeper.systemtest.ingest.IngestMode;

import java.util.Objects;
import java.util.function.Predicate;

public enum SystemTestProperty implements InstanceProperty {
    NUMBER_OF_WRITERS("sleeper.systemtest.writers", null, Objects::nonNull),
    NUMBER_OF_RECORDS_PER_WRITER("sleeper.systemtest.records-per-writer", null, Objects::nonNull),
    INGEST_MODE("sleeper.systemtest.ingest.mode", null, s -> EnumUtils.isValidEnumIgnoreCase(IngestMode.class, s)),
    SYSTEM_TEST_CLUSTER_NAME("sleeper.systemtest.cluster"),
    SYSTEM_TEST_REPO("sleeper.systemtest.repo", null, Objects::nonNull),
    WRITE_DATA_TASK_DEFINITION_FAMILY("sleeper.systemtest.task-definition"),
    SYSTEM_TEST_TASK_CPU("sleeper.systemtest.task.cpu", "1024"),
    SYSTEM_TEST_TASK_MEMORY("sleeper.systemtest.task.memory", "4096"),
    MIN_RANDOM_INT("sleeper.systemtest.random.int.min", "0"),
    MAX_RANDOM_INT("sleeper.systemtest.random.int.max", "100000000"),
    MIN_RANDOM_LONG("sleeper.systemtest.random.long.min", "0"),
    MAX_RANDOM_LONG("sleeper.systemtest.random.long.max", "10000000000"),
    RANDOM_STRING_LENGTH("sleeper.systemtest.random.string.length", "10"),
    RANDOM_BYTE_ARRAY_LENGTH("sleeper.systemtest.random.bytearray.length", "10"),
    MAX_ENTRIES_RANDOM_MAP("sleeper.systemtest.random.map.length", "10"),
    MAX_ENTRIES_RANDOM_LIST("sleeper.systemtest.random.list.length", "10");

    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;

    SystemTestProperty(String propertyName) {
        this(propertyName, null);
    }

    SystemTestProperty(String propertyName, String defaultValue) {
        this(propertyName, defaultValue, (s) -> true);
    }

    SystemTestProperty(String propertyName, String defaultValue, Predicate<String> validationPredicate) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.validationPredicate = validationPredicate;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public Predicate<String> validationPredicate() {
        return validationPredicate;
    }
}
