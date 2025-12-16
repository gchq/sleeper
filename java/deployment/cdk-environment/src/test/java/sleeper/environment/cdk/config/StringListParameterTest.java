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
import static sleeper.environment.cdk.config.AppParameters.AUTO_SHUTDOWN_EXISTING_EC2_IDS;

public class StringListParameterTest {

    @Test
    public void allowEmptyString() {
        AppContext context = AppContext.of(AUTO_SHUTDOWN_EXISTING_EC2_IDS.value(""));
        assertThat(context.get(AUTO_SHUTDOWN_EXISTING_EC2_IDS)).isEmpty();
    }

    @Test
    public void allowUnset() {
        AppContext context = AppContext.empty();
        assertThat(context.get(AUTO_SHUTDOWN_EXISTING_EC2_IDS)).isEmpty();
    }

    @Test
    public void canSetOneValue() {
        AppContext context = AppContext.of(AUTO_SHUTDOWN_EXISTING_EC2_IDS.value("a-value"));
        assertThat(context.get(AUTO_SHUTDOWN_EXISTING_EC2_IDS))
                .containsExactly("a-value");
    }

    @Test
    public void canSetMultipleValues() {
        AppContext context = AppContext.of(AUTO_SHUTDOWN_EXISTING_EC2_IDS.value("value-1", "value-2"));
        assertThat(context.get(AUTO_SHUTDOWN_EXISTING_EC2_IDS))
                .containsExactly("value-1", "value-2");
    }

}
