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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class InternalModuleIndex {

    private final Map<ArtifactReference, MavenModuleAndPath> modulesByArtifactRef;
    private final Map<String, MavenModuleAndPath> modulesByPath;

    InternalModuleIndex(List<MavenModuleAndPath> paths) {
        modulesByArtifactRef = paths.stream()
                .collect(Collectors.toMap(path -> path.getStructure().artifactReference(), path -> path));
        modulesByPath = paths.stream()
                .collect(Collectors.toMap(MavenModuleAndPath::getPath, path -> path));
    }

    public Stream<String> dependencyPathsForModules(String... paths) {
        return dependenciesForModules(Arrays.asList(paths))
                .map(MavenModuleAndPath::getPath);
    }

    public Stream<String> dependencyPathsForModulesExcludingUnexportedTransitives(String... paths) {
        return dependenciesForModulesExcludingUnexportedTransitives(Arrays.asList(paths))
                .map(MavenModuleAndPath::getPath);
    }

    public Stream<MavenModuleAndPath> dependenciesForModules(List<String> paths) {
        return modulesAndAllDependencies(modulesByPathOrThrow(paths));
    }

    public Stream<MavenModuleAndPath> dependenciesForModulesExcludingUnexportedTransitives(List<String> paths) {
        return modulesAndAllDirectOrExportedDependencies(modulesByPathOrThrow(paths));
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

    private Stream<MavenModuleAndPath> modulesAndAllDependencies(Stream<MavenModuleAndPath> modules) {
        return modulesAndAllDependencies(modules, MavenModuleAndPath::dependencies);
    }

    private Stream<MavenModuleAndPath> modulesAndAllDirectOrExportedDependencies(Stream<MavenModuleAndPath> modules) {
        return modulesAndAllDependencies(modules, MavenModuleAndPath::exportedDependencies);
    }

    private Stream<MavenModuleAndPath> modulesAndAllDependencies(
            Stream<MavenModuleAndPath> modules,
            Function<MavenModuleAndPath, Stream<DependencyReference>> getTransitiveDependencies) {
        List<MavenModuleAndPath> list = modules.collect(Collectors.toList());
        return Stream.concat(list.stream(),
                removeDuplicatesLastFirst(
                        list.stream()
                                .flatMap(MavenModuleAndPath::dependencies)
                                .flatMap(dependencyRef -> streamByArtifactRef(dependencyRef.artifactReference()))
                                .flatMap(dependency -> traverseInternalTransitives(dependency, getTransitiveDependencies)),
                        list));
    }

    private Stream<MavenModuleAndPath> traverseInternalTransitives(
            MavenModuleAndPath root,
            Function<MavenModuleAndPath, Stream<DependencyReference>> getTransitiveDependencies) {
        List<MavenModuleAndPath> breadthFirstOrder = new ArrayList<>();
        Queue<MavenModuleAndPath> queue = new LinkedList<>();
        for (MavenModuleAndPath module = root; module != null; module = queue.poll()) {
            breadthFirstOrder.add(module);
            getTransitiveDependencies.apply(module)
                    .map(DependencyReference::artifactReference)
                    .flatMap(this::streamByArtifactRef)
                    .forEach(queue::add);
        }
        return breadthFirstOrder.stream();
    }

    private Stream<MavenModuleAndPath> traverseAncestors(MavenModuleAndPath path) {
        return streamByArtifactRef(path.getParentReference())
                .flatMap(module -> Stream.concat(traverseAncestors(module), Stream.of(module)));
    }

    public Stream<MavenModuleAndPath> lookupDependencies(Stream<DependencyReference> dependencies) {
        return dependencies.map(DependencyReference::artifactReference)
                .flatMap(this::streamByArtifactRef);
    }

    public Stream<MavenModuleAndPath> streamByArtifactRef(ArtifactReference reference) {
        return Stream.of(moduleByArtifactRef(reference))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public Optional<MavenModuleAndPath> moduleByArtifactRef(ArtifactReference reference) {
        return Optional.ofNullable(reference)
                .map(modulesByArtifactRef::get);
    }

    private static <T> Stream<T> removeDuplicatesLastFirst(Stream<T> stream, Collection<T> exclude) {
        return removeDuplicatesLastFirst(
                stream.collect(Collectors.toList()), exclude)
                .stream();
    }

    private static <T> List<T> removeDuplicatesLastFirst(List<T> list, Collection<T> exclude) {
        Set<T> set = new HashSet<>(exclude);
        LinkedList<T> newList = new LinkedList<>();
        reverseStream(list).forEach(item -> {
            if (set.add(item)) {
                newList.addFirst(item);
            }
        });
        return newList;
    }

    private static <T> Stream<T> reverseStream(List<T> list) {
        int size = list.size();
        return IntStream.rangeClosed(1, size)
                .mapToObj(i -> list.get(size - i));
    }
}
