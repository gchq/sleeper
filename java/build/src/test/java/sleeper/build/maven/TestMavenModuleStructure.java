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

public class TestMavenModuleStructure {

    private TestMavenModuleStructure() {
    }

    public static MavenModuleStructure example() {
        return rootBuilder().modulesArray(
                testedModuleBuilder("core").build(),
                testedModuleBuilder("configuration").dependenciesArray(
                        dependency("org.apache.datasketches:datasketches-java"),
                        dependency("sleeper:core"),
                        dependency("junit:junit"),
                        dependency("sleeper:core")
                ).build(),
                testedModuleBuilder("ingest").dependenciesArray(
                        dependency("org.apache.commons:commons-lang3"),
                        dependency("sleeper:configuration"),
                        dependency("org.testcontainers:testcontainers"),
                        dependency("sleeper:core"),
                        dependency("sleeper:configuration")
                ).build(),
                midParentBuilder("bulk-import").modulesArray(
                        testedModuleBuilder("bulk-import-common").dependenciesArray(
                                dependency("sleeper:configuration"),
                                dependency("net.javacrumbs.json-unit:json-unit-assertj")
                        ).build(),
                        testedModuleBuilder("bulk-import-runner").dependenciesArray(
                                dependency("sleeper:bulk-import-common"),
                                dependency("sleeper:ingest"),
                                dependency("sleeper:configuration"),
                                dependency("sleeper:core")
                        ).build(),
                        testedModuleBuilder("bulk-import-starter").dependenciesArray(
                                dependency("sleeper:bulk-import-common"),
                                dependency("sleeper:core")
                        ).build()
                ).build(),
                untestedModuleBuilder("distribution").build()
        ).build();
    }

    public static MavenModuleStructure.Builder rootBuilder() {
        return artifactIdAndRefBuilder("parent", null).packaging("pom");
    }

    public static MavenModuleStructure.Builder midParentBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId).packaging("pom");
    }

    public static MavenModuleStructure.Builder testedModuleBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId).hasSrcTestFolder(true);
    }

    public static MavenModuleStructure.Builder untestedModuleBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId).hasSrcTestFolder(false);
    }

    public static MavenModuleStructure.Builder artifactIdAndRefBuilder(String artifactId) {
        return artifactIdAndRefBuilder(artifactId, artifactId);
    }

    public static MavenModuleStructure.Builder artifactIdAndRefBuilder(String artifactId, String moduleRef) {
        return MavenModuleStructure.builder().artifactId(artifactId).moduleRef(moduleRef).groupId("sleeper");
    }

    public static DependencyReference dependency(String ref) {
        String[] parts = ref.split(":");
        return DependencyReference.groupAndArtifact(parts[0], parts[1]);
    }
}
