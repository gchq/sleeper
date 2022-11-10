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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalDependencyIndex {

    private final Map<DependencyReference, MavenModuleAndPath> modulesByDependencyRef;
    private final Map<String, MavenModuleAndPath> modulesByPath;

    InternalDependencyIndex(List<MavenModuleAndPath> paths) {
        modulesByDependencyRef = paths.stream()
                .collect(Collectors.toMap(path -> path.getStructure().asDependency(), path -> path));
        modulesByPath = paths.stream()
                .collect(Collectors.toMap(MavenModuleAndPath::getPath, path -> path));
    }

    public Stream<String> dependenciesForModules(String... paths) {
        return Stream.of(paths)
                .flatMap(this::dependenciesOfPath)
                .distinct();
    }

    private Stream<String> dependenciesOfPath(String path) {
        MavenModuleAndPath module = moduleByPath(path)
                .orElseThrow(() -> new IllegalArgumentException("Module not found: " + path));
        return moduleAndDependencies(module)
                .map(MavenModuleAndPath::getPath);
    }

    private Optional<MavenModuleAndPath> moduleByPath(String path) {
        return Optional.ofNullable(modulesByPath.get(path));
    }

    private Stream<MavenModuleAndPath> moduleAndDependencies(MavenModuleAndPath path) {
        return Stream.concat(Stream.of(path), dependencies(path));
    }

    private Stream<MavenModuleAndPath> dependencies(MavenModuleAndPath path) {
        return path.getStructure().dependencies()
                .flatMap(this::dependenciesByRef);
    }

    private Optional<MavenModuleAndPath> moduleByDependencyRef(DependencyReference reference) {
        return Optional.ofNullable(modulesByDependencyRef.get(reference));
    }

    private Stream<MavenModuleAndPath> dependenciesByRef(DependencyReference reference) {
        return Stream.of(moduleByDependencyRef(reference))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(this::moduleAndDependencies);
    }
}
