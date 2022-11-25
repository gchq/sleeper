/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.maven.TestMavenModuleStructure.dependency;
import static sleeper.build.maven.TestMavenModuleStructure.dependencyBuilder;
import static sleeper.build.maven.TestMavenModuleStructure.testedModuleBuilder;

public class InternalDependencyIndexTest {

    @Test
    public void shouldProduceListOfDependenciesFromModuleWithNoInternalDependencies() {
        // Given
        InternalDependencyIndex index = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThat(index.dependencyPathsForModules("core"))
                .containsExactly("core");
    }

    @Test
    public void shouldProduceListOfDependenciesFromModulesWithNoOtherInternalDependencies() {
        // Given
        InternalDependencyIndex index = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThat(index.dependencyPathsForModules("core", "configuration"))
                .containsExactly("core", "configuration");
    }

    @Test
    public void shouldProduceListOfDependenciesFromModulesWithInternalDependencies() {
        // Given
        InternalDependencyIndex index = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThat(index.dependencyPathsForModules("ingest"))
                .containsExactly("ingest", "configuration", "core");
    }

    @Test
    public void shouldProduceListOfDependenciesFromNestedModulesWithInternalDependencies() {
        // Given
        InternalDependencyIndex index = TestMavenModuleStructure.example().internalDependencies();

        // When / Then
        assertThat(index.dependencyPathsForModules(
                "bulk-import/bulk-import-common",
                "bulk-import/bulk-import-runner",
                "bulk-import/bulk-import-starter"))
                .containsExactly(
                        "bulk-import/bulk-import-common", "configuration", "core",
                        "bulk-import/bulk-import-runner", "ingest",
                        "bulk-import/bulk-import-starter");
    }

    @Test
    public void shouldIncludeTransitiveOnlyDependency() {
        // Given
        InternalDependencyIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                testedModuleBuilder("a").build(),
                testedModuleBuilder("b").dependenciesArray(dependency("sleeper:a")).build(),
                testedModuleBuilder("c").dependenciesArray(dependency("sleeper:b")).build()
        ).build().internalDependencies();

        // When / Then
        assertThat(index.dependencyPathsForModules("c"))
                .containsExactly("c", "b", "a");
    }

    @Test
    public void shouldIncludeDependencyWithExplicitlyDeclaredScope() {
        // Given
        InternalDependencyIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                testedModuleBuilder("a").build(),
                testedModuleBuilder("b").dependenciesArray(
                        dependencyBuilder("sleeper:a").scope("something").exported(true).build()).build()
        ).build().internalDependencies();

        // When / Then
        assertThat(index.dependencyPathsForModules("b"))
                .containsExactly("b", "a");
    }

    @Test
    public void shouldExcludeDependencyWithWrongGroup() {
        // Given
        InternalDependencyIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                testedModuleBuilder("a").build(),
                testedModuleBuilder("b").dependenciesArray(dependency("abc:a")).build()
        ).build().internalDependencies();

        // When / Then
        assertThat(index.dependencyPathsForModules("b"))
                .containsExactly("b");
    }
}
