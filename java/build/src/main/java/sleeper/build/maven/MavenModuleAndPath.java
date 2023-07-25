/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.build.chunks.ProjectStructure;

import java.nio.file.Path;
import java.util.stream.Stream;

public class MavenModuleAndPath {
    private final String path;
    private final ArtifactReference parent;
    private final MavenModuleStructure structure;

    private MavenModuleAndPath(String path, ArtifactReference parent, MavenModuleStructure structure) {
        this.path = path;
        this.parent = parent;
        this.structure = structure;
    }

    public MavenModuleAndPath child(MavenModuleStructure structure) {
        return new MavenModuleAndPath(
                projectListPathFromParent(this, structure),
                this.structure.artifactReference(),
                structure);
    }

    public static MavenModuleAndPath root(MavenModuleStructure structure) {
        return new MavenModuleAndPath(null, null, structure);
    }

    public Stream<MavenModuleAndPath> thisAndDescendents() {
        return Stream.concat(Stream.of(this), descendents());
    }

    public Stream<MavenModuleAndPath> descendents() {
        return structure.childModules().map(this::child)
                .flatMap(MavenModuleAndPath::thisAndDescendents);
    }

    public String getPath() {
        return path;
    }

    public String getPomPath() {
        if (path == null) {
            return "pom.xml";
        } else {
            return path + "/pom.xml";
        }
    }

    public ArtifactReference artifactReference() {
        return structure.artifactReference();
    }

    public ArtifactReference getParentReference() {
        return parent;
    }

    public Path pathInRepository(ProjectStructure project) {
        return project.relativizeMavenPathInRepository(path);
    }

    public Path pomPathInRepository(ProjectStructure project) {
        return project.relativizeMavenPathInRepository(getPomPath());
    }

    public String getModuleRef() {
        return structure.getModuleRef();
    }

    public MavenModuleStructure getStructure() {
        return structure;
    }

    public Stream<DependencyReference> dependencies() {
        return structure.dependencies();
    }

    public Stream<DependencyReference> exportedDependencies() {
        return dependencies().filter(DependencyReference::isExported);
    }

    private static String projectListPathFromParent(MavenModuleAndPath parent, MavenModuleStructure structure) {
        if (parent.path != null) {
            return parent.path + "/" + structure.getModuleRef();
        } else {
            return structure.getModuleRef();
        }
    }

    public String toString() {
        return path;
    }
}
