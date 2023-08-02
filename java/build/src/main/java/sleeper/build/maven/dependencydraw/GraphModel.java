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

public class GraphModel {

    private final List<GraphNode> nodes;
    private final List<GraphEdge> edges;
    private final Map<ArtifactReference, GraphNode> nodeByRef;

    private GraphModel(List<GraphNode> nodes, List<GraphEdge> edges) {
        this.nodes = nodes;
        this.edges = edges;
        this.nodeByRef = nodes.stream().collect(Collectors.toMap(GraphNode::getArtifactReference, node -> node));
    }

    public static GraphModel from(MavenModuleStructure structure) {
        List<MavenModuleAndPath> modules = structure.allModules()
                .collect(Collectors.toUnmodifiableList());
        InternalModuleIndex moduleIndex = structure.indexInternalModules();
        List<GraphEdge> edges = modules.stream()
                .flatMap(module -> buildEdges(module, moduleIndex))
                .collect(Collectors.toUnmodifiableList());
        List<GraphNode> nodes = modules.stream()
                .map(GraphNode::new)
                .collect(Collectors.toUnmodifiableList());
        return new GraphModel(nodes, edges);
    }

    private static Stream<GraphEdge> buildEdges(
            MavenModuleAndPath module,
            InternalModuleIndex moduleIndex) {
        Set<ArtifactReference> transitiveDependencies = module.transitiveInternalDependencies(moduleIndex)
                .map(MavenModuleAndPath::artifactReference)
                .collect(Collectors.toSet());
        List<ArtifactReference> edgeDependencies = module.internalExportedDependencies()
                .map(DependencyReference::artifactReference)
                .filter(not(transitiveDependencies::contains))
                .collect(Collectors.toUnmodifiableList());
        return edgeDependencies.stream()
                .flatMap(moduleIndex::streamByArtifactRef)
                .map(dependency -> new GraphEdge(module, dependency));
    }

    public List<GraphNode> getNodes() {
        return nodes;
    }

    public List<GraphEdge> getEdges() {
        return edges;
    }

    public GraphNode getNode(ArtifactReference artifactReference) {
        return nodeByRef.get(artifactReference);
    }
}
