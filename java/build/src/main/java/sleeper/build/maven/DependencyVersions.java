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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import sleeper.build.maven.MavenPom.ChildModule;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DependencyVersions {

    private final Map<Dependency, Set<String>> dependencyToVersions;

    private DependencyVersions(Builder builder) {
        this.dependencyToVersions = builder.dependencyToVersions;
    }

    public static DependencyVersions fromProjectBase(Path path) {
        ObjectMapper mapper = new XmlMapper();
        MavenPom pom = MavenPom.from(mapper, path.resolve("pom.xml"));
        Builder builder = builder();
        builder.index(pom);
        pom.readDescendentModules(mapper, path).map(ChildModule::pom).forEach(builder::index);
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public record Dependency(String groupId, String artifactId) {
    }

    @Override
    public int hashCode() {
        return Objects.hash(dependencyToVersions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DependencyVersions)) {
            return false;
        }
        DependencyVersions other = (DependencyVersions) obj;
        return Objects.equals(dependencyToVersions, other.dependencyToVersions);
    }

    @Override
    public String toString() {
        return "DependencyVersions{dependencyToVersions=" + dependencyToVersions + "}";
    }

    public static class Builder {
        private Map<String, String> properties = new HashMap<>();
        private Map<Dependency, Set<String>> dependencyToVersions = new HashMap<>();

        private Builder() {
        }

        public Builder dependency(String groupId, String artifactId, String version) {
            Dependency dependency = new Dependency(groupId, artifactId);
            dependencyToVersions.computeIfAbsent(dependency, d -> new HashSet<>()).add(version);
            return this;
        }

        private void index(MavenPom pom) {
            properties.putAll(pom.getProperties());
            pom.getDependencyManagement().getDependencies().forEach(this::index);
            pom.getDependencies().forEach(this::index);
        }

        private void index(MavenPom.Dependency dependency) {
            if (dependency.getGroupId().equals("sleeper") || dependency.getVersion() == null) {
                return;
            }
            dependency(dependency.getGroupId(),
                    resolveProperties(dependency.getArtifactId()),
                    resolveProperties(dependency.getVersion()));
        }

        private String resolveProperties(String string) {
            return MavenProperties.resolve(string, properties);
        }

        public DependencyVersions build() {
            return new DependencyVersions(this);
        }
    }
}
