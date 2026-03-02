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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class PersistentEMRManagedScalingBoundsTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @Test
    public void shouldValidateFalseMinLessZero() {
        // Given
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, -5);
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, 5);

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining("-5");
    }

    @Test
    public void shouldValidateFalseMaxLessZero() {
        // Given
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, 5);
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, -5);

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining("-5");
    }

    @Test
    public void shouldValidateFalseMinGreater2k() {
        // Given
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, 2001);
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, 2005);

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining("2001");
    }

    @Test
    public void shouldValidateFalseMaxGreater2k() {
        // Given
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, 100);
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, 2005);

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining("2005");
    }

    @Test
    public void shouldValidateFalseMinGreaterMax() {
        // Given
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, 10);
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, 5);

        // When / Then
        assertThatThrownBy(() -> instanceProperties.validate())
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining("5");
    }

    @Test
    public void shouldValidateTrue() {
        // Given
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, 1);
        instanceProperties.setNumber(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, 5);

        // When / Then
        instanceProperties.validate();
    }

    @Test
    public void shouldThrowOnIllegalMaxCore() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(5, 10);

        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> {
            bounds.boundMaxCoreCapacityUnits(0);
        }).withMessage("maximumCoreCapacityUnits must be > 0");
    }

    @Test
    public void shouldBoundMaxCoreWhenLargerThanMaxBound() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(1, 5);

        // When
        int maxCore = bounds.boundMaxCoreCapacityUnits(10);

        // Then
        assertThat(maxCore).isEqualTo(5);
    }

    @Test
    public void shouldNotBoundMaxCoreWhenLessThanMaxBound() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(1, 5);

        // When
        int maxCore = bounds.boundMaxCoreCapacityUnits(3);

        // Then
        assertThat(maxCore).isEqualTo(3);
    }
}
