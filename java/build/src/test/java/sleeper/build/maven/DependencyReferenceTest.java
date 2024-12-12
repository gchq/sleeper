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

import sleeper.build.testutil.TestResources;

import java.io.Reader;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.maven.TestMavenModuleStructure.dependency;
import static sleeper.build.maven.TestMavenModuleStructure.dependencyBuilder;

public class DependencyReferenceTest {

    @Test
    public void canReadDependencies() throws Exception {
        try (Reader reader = TestResources.exampleReader("examples/maven/configuration/pom.xml")) {
            MavenPom pom = MavenPom.from(reader);
            assertThat(pom.getDependencies().stream().map(DependencyReference::from)).containsExactly(
                    dependency("org.apache.datasketches:datasketches-java"),
                    dependency("sleeper:core"),
                    dependencyBuilder("org.junit.jupiter:junit-jupiter-api").exported(false).build(),
                    dependencyBuilder("sleeper:core").type("test-jar").exported(false).build());
        }
    }
}
