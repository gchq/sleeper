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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class PersistentEMRManagedScalingBoundsTest {

    @Test
    public void shouldValidateFalseMinLessZero() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(-5, 5);

        // When
        boolean valid = bounds.isValid();

        // Then
        assertThat(valid).isFalse();
    }

    @Test
    public void shouldValidateFalseMaxLessZero() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(5, -5);

        // When
        boolean valid = bounds.isValid();

        // Then
        assertThat(valid).isFalse();
    }

    @Test
    public void shouldValidateFalseMinGreater2k() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(2001, 2005);

        // When
        boolean valid = bounds.isValid();

        // Then
        assertThat(valid).isFalse();
    }

    @Test
    public void shouldValidateFalseMaxGreater2k() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(100, 2005);

        // When
        boolean valid = bounds.isValid();

        // Then
        assertThat(valid).isFalse();
    }

    @Test
    public void shouldValidateFalseMinGreaterMax() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(10, 5);

        // When
        boolean valid = bounds.isValid();

        // Then
        assertThat(valid).isFalse();
    }

    @Test
    public void shouldValidateTrue() {
        // Given
        PersistentEMRManagedScalingBounds bounds = new PersistentEMRManagedScalingBounds(1, 5);

        // When
        boolean valid = bounds.isValid();

        // Then
        assertThat(valid).isTrue();
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
