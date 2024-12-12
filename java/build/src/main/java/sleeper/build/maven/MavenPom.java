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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MavenPom {

    private final String artifactId;
    private final String groupId;
    private final ParentRef parent;
    private final String packaging;
    private final List<String> modules;
    private final Map<String, String> properties;
    private final List<Dependency> dependencies;
    private final DependencyManagement dependencyManagement;
    private final Build build;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MavenPom(
            @JsonProperty("artifactId") String artifactId,
            @JsonProperty("groupId") String groupId,
            @JsonProperty("parent") ParentRef parent,
            @JsonProperty("packaging") String packaging,
            @JsonProperty("modules") List<String> modules,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("dependencies") List<Dependency> dependencies,
            @JsonProperty("dependencyManagement") DependencyManagement dependencyManagement,
            @JsonProperty("build") Build build) {
        this.artifactId = artifactId;
        this.groupId = groupId;
        this.parent = parent;
        this.packaging = packaging;
        this.modules = modules == null ? Collections.emptyList() : modules;
        this.properties = properties == null ? Collections.emptyMap() : properties;
        this.dependencies = dependencies == null ? Collections.emptyList() : dependencies;
        this.dependencyManagement = dependencyManagement == null ? new DependencyManagement(Collections.emptyList()) : dependencyManagement;
        this.build = build == null ? new Build(Collections.emptyList(), new PluginManagement(Collections.emptyList())) : build;
    }

    public static MavenPom from(ObjectMapper mapper, Path path) {
        try (Reader reader = Files.newBufferedReader(path)) {
            return from(mapper, reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static MavenPom from(Reader reader) {
        ObjectMapper mapper = new XmlMapper();
        return from(mapper, reader);
    }

    private static MavenPom from(ObjectMapper mapper, Reader reader) {
        try {
            return mapper.readValue(reader, MavenPom.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Stream<ChildModule> readChildModules(ObjectMapper mapper, Path thisModulePath) {
        return modules.stream()
                .map(moduleRef -> ChildModule.fromParent(mapper, thisModulePath, moduleRef));
    }

    public Stream<ChildModule> readDescendentModules(ObjectMapper mapper, Path thisModulePath) {
        return readChildModules(mapper, thisModulePath)
                .flatMap(module -> Stream.concat(
                        Stream.of(module),
                        module.pom().readDescendentModules(mapper, module.path())));
    }

    public record ChildModule(String moduleRef, Path path, MavenPom pom) {

        public static ChildModule fromParent(ObjectMapper mapper, Path parentPath, String moduleRef) {
            Path path = parentPath.resolve(moduleRef);
            MavenPom pom = MavenPom.from(mapper, path.resolve("pom.xml"));
            return new ChildModule(moduleRef, path, pom);
        }
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getGroupId() {
        if (groupId != null) {
            return groupId;
        }
        if (parent != null) {
            return parent.groupId;
        }
        return null;
    }

    public String getPackaging() {
        return packaging;
    }

    public List<String> getModules() {
        return modules;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<Dependency> getDependencies() {
        return dependencies;
    }

    public DependencyManagement getDependencyManagement() {
        return dependencyManagement;
    }

    public Build getBuild() {
        return build;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ParentRef {
        private final String groupId;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public ParentRef(
                @JsonProperty("groupId") String groupId) {
            this.groupId = groupId;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DependencyManagement {
        private final List<Dependency> dependencies;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public DependencyManagement(
                @JsonProperty("dependencies") List<Dependency> dependencies) {
            this.dependencies = dependencies == null ? Collections.emptyList() : dependencies;
        }

        public List<Dependency> getDependencies() {
            return dependencies;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Build {
        private final List<Plugin> plugins;
        private final PluginManagement pluginManagement;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Build(
                @JsonProperty("plugins") List<Plugin> plugins,
                @JsonProperty("pluginManagement") PluginManagement pluginManagement) {
            this.plugins = plugins == null ? Collections.emptyList() : plugins;
            this.pluginManagement = pluginManagement == null ? new PluginManagement(Collections.emptyList()) : pluginManagement;
        }

        public List<Plugin> getPlugins() {
            return plugins;
        }

        public PluginManagement getPluginManagement() {
            return pluginManagement;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PluginManagement {
        private final List<Plugin> plugins;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public PluginManagement(
                @JsonProperty("plugins") List<Plugin> plugins) {
            this.plugins = plugins == null ? Collections.emptyList() : plugins;
        }

        public List<Plugin> getPlugins() {
            return plugins;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Dependency {
        private final String artifactId;
        private final String groupId;
        private final String version;
        private final String type;
        private final String scope;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Dependency(
                @JsonProperty("artifactId") String artifactId,
                @JsonProperty("groupId") String groupId,
                @JsonProperty("version") String version,
                @JsonProperty("type") String type,
                @JsonProperty("scope") String scope) {
            this.artifactId = artifactId;
            this.groupId = groupId;
            this.version = version;
            this.type = type;
            this.scope = scope;
        }

        public String getArtifactId() {
            return artifactId;
        }

        public String getGroupId() {
            return groupId;
        }

        public String getVersion() {
            return version;
        }

        public String getType() {
            return type;
        }

        public String getScope() {
            return scope;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Plugin {
        private final String artifactId;
        private final String groupId;
        private final String version;
        private final List<Dependency> dependencies;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Plugin(
                @JsonProperty("artifactId") String artifactId,
                @JsonProperty("groupId") String groupId,
                @JsonProperty("version") String version,
                @JsonProperty("dependencies") List<Dependency> dependencies) {
            this.artifactId = artifactId;
            this.groupId = groupId;
            this.version = version;
            this.dependencies = dependencies == null ? Collections.emptyList() : dependencies;
        }

        public String getArtifactId() {
            return artifactId;
        }

        public String getGroupId() {
            return groupId;
        }

        public String getVersion() {
            return version;
        }

        public List<Dependency> getDependencies() {
            return dependencies;
        }
    }
}
