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

import java.util.stream.Stream;

public class MavenProjectListPath {

    private final String path;
    private final MavenModuleStructure structure;

    private MavenProjectListPath(String path, MavenModuleStructure structure) {
        this.path = path;
        this.structure = structure;
    }

    public MavenProjectListPath child(MavenModuleStructure structure) {
        return new MavenProjectListPath(
                projectListPathFromParent(this, structure),
                structure);
    }

    public static MavenProjectListPath root(MavenModuleStructure structure) {
        return new MavenProjectListPath(null, structure);
    }

    public Stream<MavenProjectListPath> thisAndDescendents() {
        return Stream.concat(Stream.of(this), descendents());
    }

    public Stream<MavenProjectListPath> descendents() {
        return structure.childModules().map(this::child)
                .flatMap(MavenProjectListPath::thisAndDescendents);
    }

    public String getPath() {
        return path;
    }

    public MavenModuleStructure getStructure() {
        return structure;
    }

    private static String projectListPathFromParent(MavenProjectListPath parent, MavenModuleStructure structure) {
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
