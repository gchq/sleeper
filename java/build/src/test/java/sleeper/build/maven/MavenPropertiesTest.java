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
package sleeper.build.maven;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class MavenPropertiesTest {

    @Test
    void shouldResolveVersionProperty() {
        assertThat(MavenProperties.resolve("${library.version}", Map.of("library.version", "1.2.3")))
                .isEqualTo("1.2.3");
    }

    @Test
    void shouldResolveScalaArtifactProperty() {
        assertThat(MavenProperties.resolve("library_${scala.version}", Map.of("scala.version", "1.2.3")))
                .isEqualTo("library_1.2.3");
    }

    @Test
    void shouldResolvePropertyReferencingAnotherProperty() {
        assertThat(MavenProperties.resolve("${failsafe.plugin.version}",
                Map.of(
                        "failsafe.plugin.version", "${surefire.plugin.version}",
                        "surefire.plugin.version", "3.5.2")))
                .isEqualTo("3.5.2");
    }

}
