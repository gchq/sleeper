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

import java.awt.Color;
import java.awt.Paint;
import java.util.Set;

public class PickedNodeState {

    private final GraphModel model;
    private PickedNodeEdges pickedNodeEdges = new PickedNodeEdges();

    public PickedNodeState(GraphModel model) {
        this.model = model;
    }

    public void updatePicked(Set<GraphNode> picked) {
        pickedNodeEdges = new PickedNodeEdges(model, picked);
    }

    public Color calculateEdgeColor(GraphEdge edge, boolean showTransitiveDependencies) {
        if (pickedNodeEdges.isDirectDependency(edge)) {
            return Color.BLUE;
        }
        if (pickedNodeEdges.isDirectDependent(edge)) {
            return Color.RED;
        }
        if (showTransitiveDependencies) {
            if (pickedNodeEdges.isTransitiveDependency(edge)) {
                return Color.BLACK;
            } else {
                return Color.lightGray;
            }
        }
        return Color.BLACK;
    }

    public Paint calculateArrowColor(GraphEdge edge, boolean showTransitiveDependencies) {
        Paint edgePaintColor = calculateEdgeColor(edge, showTransitiveDependencies);
        if (edgePaintColor.equals(Color.lightGray)) {
            return new Color(255, 255, 255, 0);
        }
        return edgePaintColor;
    }
}
