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

import java.util.List;

public class GraphData {
    List<String> nodeIds;
    List<List<String>> edges;

    GraphData(List<String> nodeIds, List<List<String>> edges) {
        this.nodeIds = nodeIds;
        this.edges = edges;
    }

    public List<String> getNodeIds() {
        return nodeIds;
    }

    public List<List<String>> getEdges() {
        return edges;
    }
}
