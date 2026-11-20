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
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.SleeperPropertyValues;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperty;

import java.util.List;

import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS;
import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS;
import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS;

/**
 * The bounds on the number of executors for Spark dynamic allocation on EMR Serverless. Spark requires that these are
 * consistent with one another, so they need to be validated together.
 *
 * @param minExecutors     the lower bound for the number of executors
 * @param initialExecutors the number of executors to start with
 * @param maxExecutors     the upper bound for the number of executors
 */
public record EmrServerlessDynamicAllocationBounds(int minExecutors, int initialExecutors, int maxExecutors) {

    /**
     * Checks whether the bounds are consistent with one another, i.e. the initial executors are between the min and max
     * executors, inclusive.
     *
     * @return true if the bounds are valid
     */
    public boolean isValid() {
        return minExecutors <= initialExecutors && initialExecutors <= maxExecutors;
    }

    /**
     * Creates a validation criteria for the instance defaults for the dynamic allocation bounds.
     *
     * @return a validation criteria that fails unless the initial executors are between the
     *         min and max executors, inclusive
     */
    public static SleeperPropertiesValidationCriteria<InstanceProperty> instanceDefaultsValidationCriteria() {
        return validationCriteria(
                DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS,
                DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS,
                DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS);
    }

    /**
     * Creates a validation criteria for the dynamic allocation bounds in table properties. This includes any values
     * inherited from the instance defaults.
     *
     * @return a validation criteria that fails unless the initial executors are between the
     *         min and max executors, inclusive
     */
    public static SleeperPropertiesValidationCriteria<TableProperty> tableValidationCriteria() {
        return validationCriteria(
                BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS,
                BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS,
                BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS);
    }

    private static <T extends SleeperProperty> SleeperPropertiesValidationCriteria<T> validationCriteria(T min, T initial, T max) {
        return SleeperPropertiesValidationCriteria.forProperties(List.of(min, initial, max),
                properties -> from(properties, min, initial, max).isValid());
    }

    private static <T extends SleeperProperty> EmrServerlessDynamicAllocationBounds from(
            SleeperPropertyValues<T> properties, T min, T initial, T max) {
        return new EmrServerlessDynamicAllocationBounds(
                properties.getInt(min), properties.getInt(initial), properties.getInt(max));
    }
}
