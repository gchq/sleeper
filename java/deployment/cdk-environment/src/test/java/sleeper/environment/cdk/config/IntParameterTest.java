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
package sleeper.environment.cdk.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IntParameterTest {
    private static final IntParameter TEST_PARAM = IntParameter.keyAndDefault("buildRootVolumeSizeGiB", 350);

    @Test
    public void useDefaultValueWhenUnset() {
        AppContext context = AppContext.empty();
        assertThat(context.get(TEST_PARAM)).isEqualTo(350);
    }

    @Test
    public void canSetValue() {
        AppContext context = AppContext.of(TEST_PARAM.value(123));
        assertThat(context.get(TEST_PARAM)).isEqualTo(123);
    }

    @Test
    public void refuseLettersInValue() {
        AppContext context = AppContext.of(TEST_PARAM.value("abc"));
        assertThatThrownBy(() -> context.get(TEST_PARAM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("buildRootVolumeSizeGiB")
                .hasCauseInstanceOf(NumberFormatException.class);
    }
}
