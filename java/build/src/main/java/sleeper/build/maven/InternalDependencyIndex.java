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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalDependencyIndex {

    private final Map<ArtifactReference, MavenModuleAndPath> modulesByArtifactRef;
    private final Map<String, MavenModuleAndPath> modulesByPath;

    InternalDependencyIndex(List<MavenModuleAndPath> paths) {
        modulesByArtifactRef = paths.stream()
                .collect(Collectors.toMap(path -> path.getStructure().artifactReference(), path -> path));
        modulesByPath = paths.stream()
                .collect(Collectors.toMap(MavenModuleAndPath::getPath, path -> path));
    }

    public Stream<String> dependencyPathsForModules(String... paths) {
        return dependenciesForModules(Arrays.asList(paths))
                .map(MavenModuleAndPath::getPath);
    }

    public Stream<MavenModuleAndPath> dependenciesForModules(List<String> paths) {
        return modulesByPathOrThrow(paths)
                .flatMap(this::moduleAndAllDependencies)
                .distinct();
    }

    public Stream<String> pomPathsForAncestors(String... paths) {
        return ancestorsForModules(Arrays.asList(paths))
                .map(MavenModuleAndPath::getPomPath);
    }

    public Stream<MavenModuleAndPath> ancestorsForModules(List<String> paths) {
        return modulesByPathOrThrow(paths)
                .flatMap(this::traverseAncestors)
                .distinct();
    }

    private Stream<MavenModuleAndPath> modulesByPathOrThrow(List<String> paths) {
        return paths.stream().map(this::moduleByPathOrThrow);
    }

    private MavenModuleAndPath moduleByPathOrThrow(String path) {
        return Optional.ofNullable(modulesByPath.get(path))
                .orElseThrow(() -> new IllegalArgumentException("Module not found: " + path));
    }

    private Stream<MavenModuleAndPath> moduleAndAllDependencies(MavenModuleAndPath path) {
        return Stream.concat(Stream.of(path),
                path.getStructure().dependencies()
                        .flatMap(this::traverseInternalTransitives));
    }

    private Stream<MavenModuleAndPath> moduleAndExportedDependencies(MavenModuleAndPath path) {
        return Stream.concat(Stream.of(path),
                path.getStructure().dependencies()
                        .filter(DependencyReference::isExported)
                        .flatMap(this::traverseInternalTransitives));
    }

    private Stream<MavenModuleAndPath> traverseInternalTransitives(DependencyReference reference) {
        return streamByArtifactRef(reference.artifactReference())
                .flatMap(this::moduleAndExportedDependencies);
    }

    private Stream<MavenModuleAndPath> traverseAncestors(MavenModuleAndPath path) {
        return streamByArtifactRef(path.getParentRef())
                .flatMap(module -> Stream.concat(traverseAncestors(module), Stream.of(module)));
    }

    private Stream<MavenModuleAndPath> streamByArtifactRef(ArtifactReference reference) {
        return Stream.of(moduleByArtifactRef(reference))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private Optional<MavenModuleAndPath> moduleByArtifactRef(ArtifactReference reference) {
        return Optional.ofNullable(reference)
                .map(modulesByArtifactRef::get);
    }
}
