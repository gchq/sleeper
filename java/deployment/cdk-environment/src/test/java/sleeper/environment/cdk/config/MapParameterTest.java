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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.environment.cdk.config.AppParameters.TAGS;

public class MapParameterTest {

    @Test
    public void allowEmptyString() {
        AppContext context = AppContext.of(TAGS.value(""));
        assertThat(context.get(TAGS)).isEmpty();
    }

    @Test
    public void allowUnset() {
        AppContext context = AppContext.empty();
        assertThat(context.get(TAGS)).isEmpty();
    }

    @Test
    public void canSetOnePair() {
        AppContext context = AppContext.of(TAGS.value("Project", "sleeper"));
        assertThat(context.get(TAGS))
                .containsExactly(Map.entry("Project", "sleeper"));
    }

    @Test
    public void canSetMultiplePairsKeepingOrder() {
        AppContext context = AppContext.of(TAGS.value("Project", "sleeper", "Owner", "alice"));
        assertThat(context.get(TAGS))
                .containsExactly(Map.entry("Project", "sleeper"), Map.entry("Owner", "alice"));
    }

    @Test
    public void refuseOddNumberOfEntries() {
        AppContext context = AppContext.of(TAGS.value("Project", "sleeper", "Owner"));
        assertThatThrownBy(() -> context.get(TAGS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void refuseEmptyValue() {
        AppContext context = AppContext.of(TAGS.value("Project", ""));
        assertThatThrownBy(() -> context.get(TAGS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void refuseEmptyKey() {
        AppContext context = AppContext.of(TAGS.value("", "sleeper"));
        assertThatThrownBy(() -> context.get(TAGS))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
