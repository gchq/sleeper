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
import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;

public class RequiredStringParameterTest {

    @Test
    public void refuseEmptyString() {
        AppContext context = AppContext.of(INSTANCE_ID.value(""));
        assertThatThrownBy(() -> context.get(INSTANCE_ID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instanceId");
    }

    @Test
    public void refuseUnset() {
        AppContext context = AppContext.empty();
        assertThatThrownBy(() -> context.get(INSTANCE_ID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instanceId");
    }

    @Test
    public void canSetValue() {
        AppContext context = AppContext.of(INSTANCE_ID.value("some-test-id"));
        assertThat(context.get(INSTANCE_ID)).contains("some-test-id");
    }

}
