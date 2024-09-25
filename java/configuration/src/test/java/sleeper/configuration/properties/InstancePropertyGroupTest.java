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

package sleeper.configuration.properties;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.instance.UserDefinedInstanceProperty;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.InstanceProperty.getAll;

public class InstancePropertyGroupTest {
    @Test
    void shouldGetAllUserDefinedAndSystemDefinedProperties() {
        // Given/When
        List<InstanceProperty> propertyList = getAll();

        // Then
        assertThat(propertyList)
                .containsAll(UserDefinedInstanceProperty.getAll())
                .containsAll(CdkDefinedInstanceProperty.getAll());
    }
}
