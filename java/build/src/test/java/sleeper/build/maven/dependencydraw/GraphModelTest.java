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

import org.junit.jupiter.api.Test;

import sleeper.build.maven.TestMavenModuleStructure;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphModelTest {

    @Test
    void shouldCreateNodesForAllModules() {
        GraphModel model = GraphModel.from(TestMavenModuleStructure.example());

        assertThat(model.getNodes())
                .extracting(GraphNode::getName)
                .containsExactly(
                        "core",
                        "configuration",
                        "ingest",
                        "bulk-import/bulk-import-common",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter",
                        "distribution");
    }

    @Test
    void shouldCreateEdgesForAllInternalDependencies() {
        GraphModel model = GraphModel.from(TestMavenModuleStructure.example());

        assertThat(model.getEdges())
                .extracting(GraphEdge::toString)
                .containsExactly(
                        "configuration > core",
                        "ingest > configuration",
                        "bulk-import/bulk-import-common > configuration",
                        "bulk-import/bulk-import-runner > bulk-import/bulk-import-common",
                        "bulk-import/bulk-import-runner > ingest",
                        "bulk-import/bulk-import-runner > configuration",
                        "bulk-import/bulk-import-starter > bulk-import/bulk-import-common");
    }
}
