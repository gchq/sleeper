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
import sleeper.build.maven.MavenModuleStructure;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GraphModel {

    private final List<GraphNode> nodes;
    private final List<GraphEdge> edges;

    private GraphModel(List<GraphNode> nodes, List<GraphEdge> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }

    public static GraphModel from(MavenModuleStructure structure) {
        List<GraphNode> nodes = GraphNode.allModulesFrom(structure);
        Map<ArtifactReference, GraphNode> nodeByRef = nodes.stream()
                .collect(Collectors.toMap(GraphNode::getArtifactReference, node -> node));
        List<GraphEdge> edges = nodes.stream()
                .flatMap(node -> node.buildEdges(nodeByRef))
                .collect(Collectors.toUnmodifiableList());
        return new GraphModel(nodes, edges);
    }

    public List<GraphNode> getNodes() {
        return nodes;
    }

    public List<GraphEdge> getEdges() {
        return edges;
    }
}
