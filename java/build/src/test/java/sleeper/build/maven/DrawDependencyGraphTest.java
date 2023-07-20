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

import org.jgrapht.alg.util.Pair;
import org.junit.jupiter.api.Test;

import sleeper.build.dependencydraw.DrawDependencyGraph;

import java.util.List;
import java.util.stream.Collectors;

public class DrawDependencyGraphTest {
    DrawDependencyGraph drawDependencyGraph = new DrawDependencyGraph();
    Pair<List<String>, List<List<String>>> graph = drawDependencyGraph.createGraph(TestMavenModuleStructure.example().allTestedModules().collect(Collectors.toList()));

    @Test
    public void checkCreateGraphCreatesCorrectNodes() {
        List<String> createGraph = TestMavenModuleStructure.example().allTestedModules().map(i -> i.artifactReference().toString()).collect(Collectors.toList());
        List<String> testGraph = graph.getFirst();
        assert createGraph.equals(testGraph);
    }
}