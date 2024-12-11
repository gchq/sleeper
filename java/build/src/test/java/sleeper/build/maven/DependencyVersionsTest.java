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

import sleeper.build.maven.DependencyVersions.Dependency;
import sleeper.build.maven.DependencyVersions.Version;
import sleeper.build.testutil.TestProjectStructure;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DependencyVersionsTest {

    @Test
    void shouldReadDependencyVersions() {
        assertThat(TestProjectStructure.example().loadDependencyVersions())
                .isEqualTo(DependencyVersions.builder()
                        .dependency("com.joom.spark", "spark-platform_2.12", "0.4.7")
                        .dependency("org.apache.datasketches", "datasketches-java", "1.2.3")
                        .dependency("org.apache.datasketches", "datasketches-java", "3.3.0")
                        .build());
    }

    @Test
    void shouldReadVersionParts() {
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("org.apache.datasketches", "datasketches-java", "3.3.0")
                .dependency("org.jboss.xnio", "xnio-api", "3.8.16.Final")
                .build();
        assertThat(versions.getDependencies())
                .extracting(Dependency::versions)
                .containsExactly(
                        List.of(new Version("3.3.0", 3)),
                        List.of(new Version("3.8.16.Final", 3)));
    }
}
