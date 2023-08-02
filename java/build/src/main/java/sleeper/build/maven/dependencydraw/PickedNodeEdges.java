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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PickedNodeEdges {
    private final List<GraphEdge> edgesFromPicked;
    private final List<GraphEdge> transitiveEdgesFromPicked;
    private final List<GraphEdge> edgesToPicked;

    public PickedNodeEdges() {
        edgesFromPicked = Collections.emptyList();
        transitiveEdgesFromPicked = Collections.emptyList();
        edgesToPicked = Collections.emptyList();
    }

    public PickedNodeEdges(GraphModel model, Collection<GraphNode> picked) {
        edgesFromPicked = picked.stream()
                .flatMap(model::edgesFrom)
                .collect(Collectors.toUnmodifiableList());
        transitiveEdgesFromPicked = picked.stream()
                .flatMap(model::transitiveInternalDependencies)
                .collect(Collectors.toUnmodifiableList());
        edgesToPicked = picked.stream()
                .flatMap(model::edgesTo)
                .collect(Collectors.toUnmodifiableList());
    }

    public boolean isDirectDependency(GraphEdge edge) {
        return edgesFromPicked.contains(edge);
    }

    public boolean isTransitiveDependency(GraphEdge edge) {
        return transitiveEdgesFromPicked.contains(edge);
    }

    public boolean isDirectDependent(GraphEdge edge) {
        return edgesToPicked.contains(edge);
    }
}
