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
package sleeper.cdk;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class UtilsResourceNameInstanceIdTest {
    InstanceProperties properties = createTestInstanceProperties();

    @Test
    void shouldConvertToLowerCase() {
        properties.set(ID, "AnInstanceId");
        assertThat(Utils.cleanInstanceId(properties))
                .isEqualTo("aninstanceid");
    }

    @Test
    void shouldReplaceDotsWithDashes() {
        properties.set(ID, "some.instance.id");
        assertThat(Utils.cleanInstanceId(properties))
                .isEqualTo("some-instance-id");
    }

}
