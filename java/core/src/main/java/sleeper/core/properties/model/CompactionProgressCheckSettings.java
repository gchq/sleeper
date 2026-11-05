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

import java.time.Duration;
import java.util.List;

import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS;

/**
 * Settings for how a compaction task checks the progress of a running compaction to detect when it has stalled.
 *
 * @param firstUpdateTimeout the maximum time to wait for the first progress update
 * @param stallTimeout       the maximum time the number of rows processed can stay unchanged
 * @param checkPeriod        the time between checks
 */
public record CompactionProgressCheckSettings(Duration firstUpdateTimeout, Duration stallTimeout, Duration checkPeriod) {

    /**
     * Reads the settings from instance properties.
     *
     * @param  instanceProperties the instance properties
     * @return                    the settings
     */
    public static CompactionProgressCheckSettings from(SleeperPropertyValues<InstanceProperty> instanceProperties) {
        return new CompactionProgressCheckSettings(
                Duration.ofSeconds(instanceProperties.getLong(COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS)),
                Duration.ofSeconds(instanceProperties.getLong(COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS)),
                Duration.ofSeconds(instanceProperties.getLong(COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS)));
    }

    /**
     * Creates a validation criteria to check the period between checks is less than both timeouts. Otherwise a stall
     * would not be detected until some time after the timeout.
     *
     * @return a validation criteria that fails if the check period is not less than both timeouts
     */
    public static SleeperPropertiesValidationCriteria<InstanceProperty> validationCriteria() {
        return SleeperPropertiesValidationCriteria.forProperties(
                List.of(COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS,
                        COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS,
                        COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS),
                properties -> from(properties).isCheckPeriodLessThanTimeouts());
    }

    private boolean isCheckPeriodLessThanTimeouts() {
        return checkPeriod.compareTo(firstUpdateTimeout) < 0
                && checkPeriod.compareTo(stallTimeout) < 0;
    }
}
