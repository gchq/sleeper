/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.properties.model;

import sleeper.core.properties.SleeperPropertiesValidationCriteria;
import sleeper.core.properties.SleeperPropertyValues;
import sleeper.core.properties.instance.InstanceProperty;

import java.util.List;

import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING;

/**
 * Stores information about the EMR managed scaling parameters for persistent EMR bulk import stacks.
 *
 * @param minCapacityUnits minimum number of EMR core/task workers
 * @param maxCapacityUnits maximum number of EMR core/task workers
 */
public record PersistentEMRManagedScalingBounds(int minCapacityUnits, int maxCapacityUnits) {

    /**
     * Find the upper bound for EMR maximum core capacity units based on the EMR managed scaling bounds.
     *
     * @param  maximumCoreCapacityUnits the proposed value for maximum core capacity units
     * @return                          the upper bound for core capacity units
     * @throws IllegalArgumentException if maximum core capacity units is less than 1
     */
    public int boundMaxCoreCapacityUnits(int maximumCoreCapacityUnits) {
        if (maximumCoreCapacityUnits < 1) {
            throw new IllegalArgumentException("maximumCoreCapacityUnits must be > 0");
        }
        return Math.min(maximumCoreCapacityUnits, maxCapacityUnits);
    }

    /**
     * Creates EMR managed scaling bounds from instance properties.
     *
     * @param  instanceProperties Sleeper instance properties
     * @return                    scaling bounds for EMR
     */
    public static PersistentEMRManagedScalingBounds create(SleeperPropertyValues<InstanceProperty> instanceProperties) {
        int minEmrCapacity = instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY);
        int maxEmrCapacity = instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY);
        return new PersistentEMRManagedScalingBounds(minEmrCapacity, maxEmrCapacity);
    }

    public static SleeperPropertiesValidationCriteria<InstanceProperty> validationCriteria() {
        return SleeperPropertiesValidationCriteria.forProperties(
                List.of(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING, BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY),
                properties -> {
                    if (!properties.getBoolean(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING)) {
                        return true;
                    }
                    PersistentEMRManagedScalingBounds bounds = PersistentEMRManagedScalingBounds.create(properties);
                    return bounds.maxCapacityUnits() > bounds.minCapacityUnits();
                });
    }
}
