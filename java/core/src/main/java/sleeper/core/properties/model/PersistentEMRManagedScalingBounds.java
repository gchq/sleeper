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

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY;

/**
 * Stores information about the EMR managed scaling parameters for persistent EMR bulk import stacks.
 *
 * @param minCapacityUnits minimum number of EMR core/task workers
 * @param maxCapacityUnits maximum number of EMR core/task workers
 */
public record PersistentEMRManagedScalingBounds(int minCapacityUnits, int maxCapacityUnits) {

    /**
     * Determines if the given parameters are valid.
     *
     * @return true if parameters are valid
     */
    public boolean isValid() {
        return minCapacityUnits > 0 &&
                maxCapacityUnits > 0 &&
                minCapacityUnits <= 2000 &&
                maxCapacityUnits <= 2000 &&
                minCapacityUnits < maxCapacityUnits;
    }

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
    public static PersistentEMRManagedScalingBounds createManagedScalingBounds(InstanceProperties instanceProperties) {
        int minEmrCapacity = instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY);
        int maxEmrCapacity = instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY);
        return new PersistentEMRManagedScalingBounds(minEmrCapacity, maxEmrCapacity);
    }
}
