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

import static sleeper.build.maven.ArtifactReference.groupAndArtifact;

public class TestMavenModuleStructure {

    private TestMavenModuleStructure() {
    }

    public static MavenModuleStructure example() {
        return rootBuilder().modulesArray(
                sourceModuleBuilder("core").build(),
                sourceModuleBuilder("configuration").dependenciesArray(
                        dependency("org.apache.datasketches:datasketches-java"),
                        dependency("sleeper:core"),
                        dependencyBuilder("org.junit.jupiter:junit-jupiter-api").scope("test").exported(false).build(),
                        dependencyBuilder("sleeper:core").type("test-jar").scope("test").exported(false).build()).build(),
                sourceModuleBuilder("ingest").dependenciesArray(
                        dependency("org.apache.commons:commons-lang3"),
                        dependency("sleeper:configuration"),
                        dependencyBuilder("org.testcontainers:testcontainers").scope("test").exported(false).build(),
                        dependencyBuilder("sleeper:core").type("test-jar").scope("test").exported(false).build(),
                        dependencyBuilder("sleeper:configuration").type("test-jar").scope("test").exported(false).build()).build(),
                midParentBuilder("bulk-import").modulesArray(
                        sourceModuleBuilder("bulk-import-core").dependenciesArray(
                                dependency("sleeper:configuration"),
                                dependencyBuilder("net.javacrumbs.json-unit:json-unit-assertj").scope("test").exported(false).build()).build(),
                        sourceModuleBuilder("bulk-import-runner").dependenciesArray(
                                dependency("sleeper:bulk-import-core"),
                                dependency("sleeper:ingest"),
                                dependency("sleeper:configuration"),
                                dependencyBuilder("sleeper:core").type("test-jar").scope("test").exported(false).build()).build(),
                        sourceModuleBuilder("bulk-import-starter").dependenciesArray(
                                dependency("sleeper:bulk-import-core"),
                                dependencyBuilder("sleeper:core").type("test-jar").scope("test").exported(false).build()).build())
                        .build(),
                resourcesModuleBuilder("distribution").build()).build();
    }

    public static MavenModuleStructure.Builder rootBuilder() {
        return artifactIdAndRefBuilder("parent", null).packaging("pom");
    }

    public static MavenModuleStructure.Builder midParentBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId).packaging("pom");
    }

    public static MavenModuleStructure.Builder sourceModuleBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId).hasSrcMainJavaFolder(true);
    }

    public static MavenModuleStructure.Builder resourcesModuleBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId).hasSrcMainJavaFolder(false);
    }

    public static MavenModuleStructure.Builder artifactIdAndRefBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId, artifactId);
    }

    public static MavenModuleStructure.Builder artifactIdAndRefBuilder(String artifactId, String moduleRef) {
        return MavenModuleStructure.builder().artifactId(artifactId).moduleRef(moduleRef).groupId("sleeper");
    }

    public static DependencyReference dependency(String ref) {
        return dependencyBuilder(ref).exported(true).build();
    }

    public static ArtifactReference moduleRef(String artifactId) {
        return groupAndArtifact("sleeper", artifactId);
    }

    public static DependencyReference.Builder dependencyBuilder(String ref) {
        String[] parts = ref.split(":");
        return DependencyReference.builder().groupId(parts[0]).artifactId(parts[1]);
    }
}
