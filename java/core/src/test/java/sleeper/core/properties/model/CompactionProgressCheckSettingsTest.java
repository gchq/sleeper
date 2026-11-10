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

import org.junit.jupiter.api.Test;

import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class CompactionProgressCheckSettingsTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @Test
    void shouldReadDefaults() {
        assertThat(CompactionProgressCheckSettings.from(instanceProperties))
                .isEqualTo(new CompactionProgressCheckSettings(
                        Duration.ofMinutes(10), Duration.ofMinutes(5), Duration.ofSeconds(30)));
    }

    @Test
    void shouldValidateWhenCheckPeriodLessThanBothTimeouts() {
        // Given
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS, 100);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS, 50);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, 49);

        // When / Then
        assertThatCode(() -> instanceProperties.validate()).doesNotThrowAnyException();
    }

    @Test
    void shouldNotValidateWhenCheckPeriodEqualToStallTimeout() {
        // Given
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS, 100);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS, 50);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, 50);

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOfSatisfying(SleeperPropertiesInvalidException.class, e -> assertThat(e.getInvalidValues())
                        .isEqualTo(Map.of(
                                COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS, "100",
                                COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS, "50",
                                COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, "50")));
    }

    @Test
    void shouldNotValidateWhenCheckPeriodGreaterThanFirstUpdateTimeout() {
        // Given
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS, 50);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS, 100);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, 60);

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOfSatisfying(SleeperPropertiesInvalidException.class, e -> assertThat(e.getInvalidValues())
                        .isEqualTo(Map.of(
                                COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS, "50",
                                COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS, "100",
                                COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, "60")));
    }

    @Test
    void shouldOnlyReportUnreadableValueWhenCheckPeriodIsNotANumber() {
        // Given
        instanceProperties.set(COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, "abc");

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOfSatisfying(SleeperPropertiesInvalidException.class, e -> assertThat(e.getInvalidValues())
                        .isEqualTo(Map.of(COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, "abc")));
    }
}
