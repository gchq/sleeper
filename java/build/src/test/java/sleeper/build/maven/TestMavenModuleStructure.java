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

    public static MavenModuleStructure example() {
        return rootArtifactIdAndModules("parent",
                leafArtifactIdAndRef("core"),
                leafArtifactIdAndRef("configuration"),
                leafArtifactIdAndRef("ingest"),
                middleArtifactIdRefAndModules("bulk-import",
                        leafArtifactIdAndRef("bulk-import-common"),
                        leafArtifactIdAndRef("bulk-import-runner"),
                        leafArtifactIdAndRef("bulk-import-starter")));
    }

    public static MavenModuleStructure rootArtifactIdAndModules(
            String artifactId, MavenModuleStructure... modules) {
        return MavenModuleStructure.builder().artifactId(artifactId).packaging("pom")
                .modulesArray(modules)
                .build();
    }

    public static MavenModuleStructure middleArtifactIdRefAndModules(
            String artifactId, MavenModuleStructure... modules) {
        return middleArtifactIdRefAndModules(artifactId, artifactId, modules);
    }

    public static MavenModuleStructure middleArtifactIdRefAndModules(
            String artifactId, String moduleRef, MavenModuleStructure... modules) {
        return MavenModuleStructure.builder().artifactId(artifactId).moduleRef(moduleRef).packaging("pom")
                .modulesArray(modules)
                .build();
    }

    public static MavenModuleStructure leafArtifactIdAndRef(String artifactId) {
        return leafArtifactIdAndRef(artifactId, artifactId);
    }

    public static MavenModuleStructure leafArtifactIdAndRef(String artifactId, String moduleRef) {
        return MavenModuleStructure.builder().artifactId(artifactId).moduleRef(moduleRef).build();
    }
}
