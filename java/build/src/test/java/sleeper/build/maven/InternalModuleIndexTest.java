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

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.maven.TestMavenModuleStructure.dependency;
import static sleeper.build.maven.TestMavenModuleStructure.dependencyBuilder;
import static sleeper.build.maven.TestMavenModuleStructure.sourceModuleBuilder;

public class InternalModuleIndexTest {

    @Test
    public void shouldProduceListOfDependenciesFromModuleWithNoInternalDependencies() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.example().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules("core")).containsExactly("core");
        assertThat(index.pomPathsForAncestors("core")).containsExactly("pom.xml");
    }

    @Test
    public void shouldProduceListOfDependenciesFromModulesWithNoOtherInternalDependencies() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.example().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules("core", "configuration"))
                .containsExactly("core", "configuration");
        assertThat(index.pomPathsForAncestors("core", "configuration"))
                .containsExactly("pom.xml");
    }

    @Test
    public void shouldProduceListOfDependenciesFromModulesWithInternalDependencies() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.example().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules("ingest"))
                .containsExactly("ingest", "configuration", "core");
        assertThat(index.pomPathsForAncestors("ingest"))
                .containsExactly("pom.xml");
    }

    @Test
    public void shouldProduceListOfDependenciesFromNestedModulesWithInternalDependencies() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.example().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules(
                "bulk-import/bulk-import-core",
                "bulk-import/bulk-import-runner",
                "bulk-import/bulk-import-starter"))
                .containsExactly(
                        "bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter",
                        "ingest", "configuration", "core");
        assertThat(index.pomPathsForAncestors(
                "bulk-import/bulk-import-core",
                "bulk-import/bulk-import-runner",
                "bulk-import/bulk-import-starter"))
                .containsExactly(
                        "pom.xml",
                        "bulk-import/pom.xml");
    }

    @Test
    public void shouldIncludeTransitiveOnlyDependency() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                sourceModuleBuilder("a").build(),
                sourceModuleBuilder("b").dependenciesArray(dependency("sleeper:a")).build(),
                sourceModuleBuilder("c").dependenciesArray(dependency("sleeper:b")).build()).build().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules("c"))
                .containsExactly("c", "b", "a");
    }

    @Test
    public void shouldIncludeDeeplyTransitiveDependency() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                sourceModuleBuilder("a").build(),
                sourceModuleBuilder("b").dependenciesArray(dependency("sleeper:a")).build(),
                sourceModuleBuilder("c").dependenciesArray(dependency("sleeper:b")).build(),
                sourceModuleBuilder("d").dependenciesArray(dependency("sleeper:c")).build(),
                sourceModuleBuilder("e").dependenciesArray(dependency("sleeper:d")).build()).build().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules("e"))
                .containsExactly("e", "d", "c", "b", "a");
    }

    @Test
    public void shouldIncludeUnexportedTransitiveAsCompileDependency() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                sourceModuleBuilder("a").build(),
                sourceModuleBuilder("b").dependenciesArray(
                        dependencyBuilder("sleeper:a").exported(false).build()).build(),
                sourceModuleBuilder("c").dependenciesArray(dependency("sleeper:b")).build()).build().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules("c"))
                .containsExactly("c", "b", "a");
    }

    @Test
    public void shouldExcludeUnexportedTransitiveWhenSpecified() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                sourceModuleBuilder("a").build(),
                sourceModuleBuilder("b").dependenciesArray(
                        dependencyBuilder("sleeper:a").exported(false).build()).build(),
                sourceModuleBuilder("c").dependenciesArray(dependency("sleeper:b")).build()).build().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModulesExcludingUnexportedTransitives("c"))
                .containsExactly("c", "b");
    }

    @Test
    public void shouldIncludeUnexportedDependencyDirectlyWhenExcludingUnexportedTransitives() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                sourceModuleBuilder("a").build(),
                sourceModuleBuilder("b").dependenciesArray(
                        dependencyBuilder("sleeper:a").exported(false).build()).build())
                .build().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModulesExcludingUnexportedTransitives("b"))
                .containsExactly("b", "a");
    }

    @Test
    public void shouldIncludeDependencyWithExplicitlyDeclaredScopeWhenExcludingUnexportedTransitives() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                sourceModuleBuilder("a").build(),
                sourceModuleBuilder("b").dependenciesArray(
                        dependencyBuilder("sleeper:a").scope("something").exported(true).build()).build())
                .build().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModulesExcludingUnexportedTransitives("b"))
                .containsExactly("b", "a");
    }

    @Test
    public void shouldExcludeDependencyWithWrongGroup() {
        // Given
        InternalModuleIndex index = TestMavenModuleStructure.rootBuilder().modulesArray(
                sourceModuleBuilder("a").build(),
                sourceModuleBuilder("b").dependenciesArray(dependency("abc:a")).build()).build().indexInternalModules();

        // When / Then
        assertThat(index.dependencyPathsForModules("b"))
                .containsExactly("b");
    }
}
