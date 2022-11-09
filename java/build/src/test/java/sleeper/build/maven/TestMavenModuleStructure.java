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
        return artifactIdAndRefBuilder("parent", null).packaging("pom").modulesArray(
                artifactIdAndRefBuilder("core").hasSrcTestFolder(true).build(),
                artifactIdAndRefBuilder("configuration").hasSrcTestFolder(true).dependenciesArray(
                        dependency("org.apache.datasketches:datasketches-java"),
                        dependency("sleeper:core"),
                        dependency("junit:junit"),
                        dependency("sleeper:core")
                ).build(),
                artifactIdAndRefBuilder("ingest").hasSrcTestFolder(true).dependenciesArray(
                        dependency("org.apache.commons:commons-lang3"),
                        dependency("sleeper:configuration"),
                        dependency("org.testcontainers:testcontainers"),
                        dependency("sleeper:core"),
                        dependency("sleeper:configuration")
                ).build(),
                artifactIdAndRefBuilder("bulk-import").packaging("pom").modulesArray(
                        artifactIdAndRefBuilder("bulk-import-common").hasSrcTestFolder(true).dependenciesArray(
                                dependency("sleeper:configuration"),
                                dependency("net.javacrumbs.json-unit:json-unit-assertj")
                        ).build(),
                        artifactIdAndRefBuilder("bulk-import-runner").hasSrcTestFolder(true).dependenciesArray(
                                dependency("sleeper:bulk-import-common"),
                                dependency("sleeper:ingest"),
                                dependency("sleeper:configuration"),
                                dependency("sleeper:core")
                        ).build(),
                        artifactIdAndRefBuilder("bulk-import-starter").hasSrcTestFolder(true).dependenciesArray(
                                dependency("sleeper:bulk-import-common"),
                                dependency("sleeper:core")
                        ).build()
                ).build(),
                artifactIdAndRefBuilder("distribution").build()
        ).build();
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
