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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MavenModuleStructure {

    private final String artifactId;
    private final String packaging;
    private final String moduleRef;
    private final boolean hasSrcTestFolder;
    private final List<MavenModuleStructure> modules;

    private MavenModuleStructure(Builder builder) {
        artifactId = builder.artifactId;
        packaging = builder.packaging;
        moduleRef = builder.moduleRef;
        hasSrcTestFolder = builder.hasSrcTestFolder;
        modules = Objects.requireNonNull(builder.modules, "modules must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static MavenModuleStructure fromProjectBase(Path path) throws IOException {
        ObjectMapper mapper = new XmlMapper();
        return builderFromPath(mapper, path).build();
    }

    public Stream<String> allTestedModulesForProjectList() {
        return allTestedModulesForProjectList(null);
    }

    private Stream<String> allTestedModulesForProjectList(String parentPath) {
        String projectListPath = projectListPathFromParent(parentPath);
        if ("pom".equals(packaging)) {
            return modules.stream()
                    .flatMap(module -> module.allTestedModulesForProjectList(projectListPath));
        } else if (hasSrcTestFolder) {
            return Stream.of(projectListPath);
        } else {
            return Stream.empty();
        }
    }

    private String projectListPathFromParent(String parentPath) {
        if (parentPath != null) {
            return parentPath + "/" + moduleRef;
        } else {
            return moduleRef;
        }
    }

    private static List<MavenModuleStructure> readChildModules(ObjectMapper mapper, Path path, Pom parent) throws IOException {
        List<MavenModuleStructure> modules = new ArrayList<>(parent.modules.size());
        for (String moduleRef : parent.modules) {
            modules.add(builderFromPath(mapper, path.resolve(moduleRef))
                    .moduleRef(moduleRef).build());
        }
        return modules;
    }

    private static Builder builderFromPath(ObjectMapper mapper, Path path) throws IOException {
        Pom pom = Pom.from(mapper, path.resolve("pom.xml"));
        return builder()
                .artifactId(pom.artifactId).packaging(pom.packaging)
                .hasSrcTestFolder(Files.isDirectory(path.resolve("src/test")))
                .modules(readChildModules(mapper, path, pom));
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
                && Objects.equals(artifactId, that.artifactId)
                && Objects.equals(packaging, that.packaging)
                && Objects.equals(moduleRef, that.moduleRef)
                && modules.equals(that.modules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(artifactId, packaging, moduleRef, hasSrcTestFolder, modules);
    }

    @Override
    public String toString() {
        return "MavenModuleStructure{" +
                "artifactId='" + artifactId + '\'' +
                ", packaging='" + packaging + '\'' +
                ", moduleRef='" + moduleRef + '\'' +
                ", hasSrcTestFolder=" + hasSrcTestFolder +
                ", modules=" + modules +
                '}';
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Pom {

        private final String artifactId;
        private final String packaging;
        private final List<String> modules;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Pom(
                @JsonProperty("artifactId") String artifactId,
                @JsonProperty("packaging") String packaging,
                @JsonProperty("modules") List<String> modules) {
            this.artifactId = artifactId;
            this.packaging = packaging;
            this.modules = modules == null ? Collections.emptyList() : modules;
        }

        public static Pom from(ObjectMapper mapper, Path path) throws IOException {
            try (Reader reader = Files.newBufferedReader(path)) {
                return mapper.readValue(reader, Pom.class);
            }
        }
    }

    public static final class Builder {
        private String artifactId;
        private String packaging;
        private String moduleRef;
        private boolean hasSrcTestFolder;
        private List<MavenModuleStructure> modules = Collections.emptyList();

        private Builder() {
        }

        public Builder artifactId(String artifactId) {
            this.artifactId = artifactId;
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

        public MavenModuleStructure build() {
            return new MavenModuleStructure(this);
        }
    }
}
