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
package sleeper.environment.cdk.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_ENABLED;

public class BooleanParameterTest {

    @Test
    public void refuseEmptyString() {
        AppContext context = AppContext.of(NIGHTLY_TEST_RUN_ENABLED.value(""));
        assertThatThrownBy(() -> context.get(NIGHTLY_TEST_RUN_ENABLED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nightlyTestsEnabled");
    }

    @Test
    public void useDefaultValueWhenUnset() {
        AppContext context = AppContext.empty();
        assertThat(context.get(NIGHTLY_TEST_RUN_ENABLED)).isFalse();
    }

    @Test
    public void canSetValue() {
        AppContext context = AppContext.of(NIGHTLY_TEST_RUN_ENABLED.value(true));
        assertThat(context.get(NIGHTLY_TEST_RUN_ENABLED)).isTrue();
    }
}
