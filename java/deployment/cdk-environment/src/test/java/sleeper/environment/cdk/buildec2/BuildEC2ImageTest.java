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
package sleeper.environment.cdk.buildec2;

import org.junit.jupiter.api.Test;

import sleeper.environment.cdk.config.AppContext;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.environment.cdk.buildec2.BuildEC2Image.NAME;

public class BuildEC2ImageTest {

    @Test
    void shouldUseUbuntu2604ImageByDefault() {
        assertThat(AppContext.empty().get(NAME))
                .isEqualTo("ubuntu/images/hvm-ssd-gp3/ubuntu-resolute-26.04-amd64-server-*");
    }
}
