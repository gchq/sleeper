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

package sleeper.build.maven.dependencydraw;

import sleeper.build.maven.ArtifactReference;
import sleeper.build.maven.DependencyReference;
import sleeper.build.maven.InternalModuleIndex;
import sleeper.build.maven.MavenModuleAndPath;
import sleeper.build.maven.MavenModuleStructure;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public class GraphNode {

    private final MavenModuleAndPath module;

    private final List<ArtifactReference> edgeDependencies;

    private GraphNode(MavenModuleAndPath module, List<ArtifactReference> edgeDependencies) {
        this.module = module;
        this.edgeDependencies = edgeDependencies;
    }

    public static List<GraphNode> allModulesFrom(MavenModuleStructure structure) {
        InternalModuleIndex moduleIndex = structure.indexInternalModules();
        return structure.allModules()
                .map(module -> from(module, moduleIndex))
                .collect(Collectors.toUnmodifiableList());
    }

    public static GraphNode from(MavenModuleAndPath module, InternalModuleIndex moduleIndex) {
        Set<ArtifactReference> transitiveDependencies = module.transitiveInternalDependencies(moduleIndex)
                .map(MavenModuleAndPath::artifactReference)
                .collect(Collectors.toSet());
        List<ArtifactReference> edgeDependencies = module.internalExportedDependencies()
                .map(DependencyReference::artifactReference)
                .filter(not(transitiveDependencies::contains))
                .collect(Collectors.toUnmodifiableList());
        return new GraphNode(module, edgeDependencies);
    }

    public String toString() {
        return module.getPath();
    }

    public ArtifactReference getArtifactReference() {
        return module.getStructure().artifactReference();
    }

    Stream<GraphEdge> buildEdges(Map<ArtifactReference, GraphNode> nodeByRef) {
        return edgeDependencies.stream()
                .filter(nodeByRef::containsKey)
                .map(dependency -> new GraphEdge(this, nodeByRef.get(dependency)));
    }
}
