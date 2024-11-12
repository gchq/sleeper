/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.systemtest.configuration;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperty;

import static org.assertj.core.api.Assertions.assertThat;

class SystemTestPropertyTest {

    @Test
    void shouldGetAllPropertiesInSystemTestPropertiesIndex() {
        SystemTestProperties properties = new SystemTestProperties();
        assertThat(properties.getPropertiesIndex().getAll())
                .containsAll(InstanceProperty.getAll())
                .containsAll(SystemTestProperty.getAll());
    }
}
