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
package sleeper.core.properties.instance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.core.properties.SleeperPropertiesInvalidException;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY;
import static sleeper.core.properties.instance.TableStateProperty.DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY;
import static sleeper.core.properties.instance.TableStateProperty.S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

class DynamoPointInTimeRecoveryTest {

    @Test
    void shouldKeepRecoveryDisabledByDefault() {
        assertThat(new InstanceProperties().getBoolean(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("recoveryProperties")
    void shouldInheritEnabledDefault(UserDefinedInstanceProperty property) {
        InstanceProperties properties = new InstanceProperties();
        properties.set(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY, "true");
        assertThat(properties.getBoolean(property)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("recoveryProperties")
    void shouldPreserveExplicitDisabledOverride(UserDefinedInstanceProperty property) {
        InstanceProperties properties = new InstanceProperties();
        properties.set(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY, "true");
        properties.set(property, "false");
        assertThat(properties.getBoolean(property)).isFalse();
        properties.unset(property);
        assertThat(properties.getBoolean(property)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("recoveryProperties")
    void shouldPreserveExplicitEnabledOverride(UserDefinedInstanceProperty property) {
        InstanceProperties properties = new InstanceProperties();
        properties.set(property, "true");
        assertThat(properties.getBoolean(property)).isTrue();
    }

    @Test
    void shouldRejectInvalidDefault() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY, "yes");
        assertThatThrownBy(properties::validate).isInstanceOf(SleeperPropertiesInvalidException.class);
    }

    @Test
    void shouldIncludeDefaultInBasicConfiguration() {
        assertThat(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY.isIncludedInBasicTemplate()).isTrue();
        assertThat(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY.isRunCdkDeployWhenChanged()).isTrue();
    }

    private static Stream<UserDefinedInstanceProperty> recoveryProperties() {
        return Stream.of(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY,
                S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY, TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY);
    }
}
