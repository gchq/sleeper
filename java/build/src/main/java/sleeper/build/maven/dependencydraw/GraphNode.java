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
import sleeper.build.maven.MavenModuleAndPath;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphNode {

    private final MavenModuleAndPath module;

    private final List<DependencyReference> internalDependencies;

    private GraphNode(MavenModuleAndPath module, List<DependencyReference> internalDependencies) {
        this.module = module;
        this.internalDependencies = internalDependencies;
    }

    public static GraphNode from(MavenModuleAndPath module) {
        return new GraphNode(module, module.dependencies()
                .filter(DependencyReference::isSleeper)
                .filter(DependencyReference::isExported)
                .collect(Collectors.toUnmodifiableList()));
    }

    public String getName() {
        return module.getPath();
    }

    public ArtifactReference getArtifactReference() {
        return module.getStructure().artifactReference();
    }

    public List<DependencyReference> getInternalDependencies() {
        return internalDependencies;
    }

    Stream<GraphEdge> buildEdges(Map<ArtifactReference, GraphNode> nodeByRef) {
        return internalDependencies.stream()
                .map(dependency -> new GraphEdge(this, nodeByRef.get(dependency.artifactReference()), dependency));
    }
}
