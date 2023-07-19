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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MavenModuleStructure {

    private final String artifactId;
    private final String groupId;
    private final String packaging;
    private final String moduleRef;
    private final boolean hasSrcTestFolder;
    private final List<MavenModuleStructure> modules;
    private final List<DependencyReference> dependencies;

    private MavenModuleStructure(Builder builder) {
        artifactId = Objects.requireNonNull(builder.artifactId, "artifactId must not be null");
        groupId = Objects.requireNonNull(builder.groupId, "groupId must not be null");
        packaging = builder.packaging;
        moduleRef = builder.moduleRef;
        hasSrcTestFolder = builder.hasSrcTestFolder;
        modules = Objects.requireNonNull(builder.modules, "modules must not be null");
        dependencies = Objects.requireNonNull(builder.dependencies, "dependencies must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static MavenModuleStructure fromProjectBase(Path path) throws IOException {
        ObjectMapper mapper = new XmlMapper();
        return builderFromPath(mapper, path).build();
    }

    public Stream<MavenModuleAndPath> allTestedModules() {
        return allTestedModules(MavenModuleAndPath.root(this));
    }

    public InternalDependencyIndex indexInternalDependencies() {
        MavenModuleAndPath root = MavenModuleAndPath.root(this);
        return new InternalDependencyIndex(
                root.thisAndDescendents().collect(Collectors.toList()));
    }

    public ArtifactReference artifactReference() {
        return ArtifactReference.groupAndArtifact(groupId, artifactId);
    }

    private Stream<MavenModuleAndPath> allTestedModules(MavenModuleAndPath parent) {
        MavenModuleAndPath projectListPath = parent.child(this);
        if ("pom".equals(packaging)) {
            return modules.stream()
                    .flatMap(module -> module.allTestedModules(projectListPath));
        } else if (hasSrcTestFolder) {
            return Stream.of(projectListPath);
        } else {
            return Stream.empty();
        }
    }

    String getModuleRef() {
        return moduleRef;
    }

    public Stream<MavenModuleStructure> childModules() {
        return modules.stream();
    }

    public Stream<DependencyReference> dependencies() {
        return dependencies.stream();
    }

    private static Builder builderFromPath(ObjectMapper mapper, Path path) throws IOException {
        MavenPom pom = MavenPom.from(mapper, path.resolve("pom.xml"));
        return builder()
                .artifactId(pom.getArtifactId()).groupId(pom.getGroupId()).packaging(pom.getPackaging())
                .hasSrcTestFolder(Files.isDirectory(path.resolve("src/test")))
                .dependencies(pom.getDependencies())
                .modules(readChildModules(mapper, path, pom));
    }

    private static List<MavenModuleStructure> readChildModules(ObjectMapper mapper, Path path, MavenPom parent) throws IOException {
        List<MavenModuleStructure> modules = new ArrayList<>(parent.getModules().size());
        for (String moduleRef : parent.getModules()) {
            modules.add(builderFromPath(mapper, path.resolve(moduleRef))
                    .moduleRef(moduleRef).build());
        }
        return modules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MavenModuleStructure that = (MavenModuleStructure) o;
        return hasSrcTestFolder == that.hasSrcTestFolder
                && artifactId.equals(that.artifactId)
                && groupId.equals(that.groupId)
                && Objects.equals(packaging, that.packaging)
                && Objects.equals(moduleRef, that.moduleRef)
                && modules.equals(that.modules)
                && dependencies.equals(that.dependencies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(artifactId, groupId, packaging, moduleRef, hasSrcTestFolder, modules, dependencies);
    }

    @Override
    public String toString() {
        return "MavenModuleStructure{" +
                "artifactId='" + artifactId + '\'' +
                ", groupId='" + groupId + '\'' +
                ", packaging='" + packaging + '\'' +
                ", moduleRef='" + moduleRef + '\'' +
                ", hasSrcTestFolder=" + hasSrcTestFolder +
                ", modules=" + modules +
                ", dependencies=" + dependencies +
                '}';
    }

    public static final class Builder {
        private String artifactId;
        private String groupId;
        private String packaging;
        private String moduleRef;
        private boolean hasSrcTestFolder;
        private List<MavenModuleStructure> modules = Collections.emptyList();
        private List<DependencyReference> dependencies = Collections.emptyList();

        private Builder() {
        }

        public Builder artifactId(String artifactId) {
            this.artifactId = artifactId;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder packaging(String packaging) {
            this.packaging = packaging;
            return this;
        }

        public Builder moduleRef(String moduleRef) {
            this.moduleRef = moduleRef;
            return this;
        }

        public Builder hasSrcTestFolder(boolean hasSrcTestFolder) {
            this.hasSrcTestFolder = hasSrcTestFolder;
            return this;
        }

        public Builder modules(List<MavenModuleStructure> modules) {
            this.modules = modules;
            return this;
        }

        public Builder modulesArray(MavenModuleStructure... modules) {
            return modules(Arrays.asList(modules));
        }

        public Builder dependencies(List<DependencyReference> dependencies) {
            this.dependencies = dependencies;
            return this;
        }

        public Builder dependenciesArray(DependencyReference... dependencies) {
            return dependencies(Arrays.asList(dependencies));
        }

        public MavenModuleStructure build() {
            return new MavenModuleStructure(this);
        }
    }
}
