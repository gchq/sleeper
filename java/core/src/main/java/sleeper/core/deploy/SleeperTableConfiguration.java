/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.deploy;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.table.TableProperties;

import java.util.List;

/**
 * Configuration to deploy a Sleeper table.
 *
 * @param properties        the table properties
 * @param initialPartitions the partitions to initialise the table when it is first created or reinitialised
 */
public record SleeperTableConfiguration(TableProperties properties, List<Partition> initialPartitions) {

    /**
     * Validates the configuration.
     *
     * @throws SleeperPropertiesInvalidException if any property is invalid
     * @throws InitialPartitionsInvalidException if the initial partition tree is invalid
     */
    public void validate() {
        SleeperTableValidationReporter validationReporter = new SleeperTableValidationReporter();
        validate(validationReporter);
        validationReporter.throwIfFailed();
    }

    /**
     * Validates the configuration.
     *
     * @param validationReporter the reporter for any validation failure
     */
    public void validate(SleeperTableValidationReporter validationReporter) {
        properties.validate(validationReporter.getPropertiesReporter());
        try {
            new PartitionTree(initialPartitions).validate(properties.getSchema());
        } catch (RuntimeException e) {
            validationReporter.initialPartitionsInvalid(e);
        }
    }

}
