/*
 * Copyright 2022-2024 Crown Copyright
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
import static sleeper.build.maven.TestMavenModuleStructure.dependency;
import static sleeper.build.maven.TestMavenModuleStructure.moduleRef;
import static sleeper.build.maven.TestMavenModuleStructure.rootBuilder;
import static sleeper.build.maven.TestMavenModuleStructure.sourceModuleBuilder;

public class GraphModelTest {

    @Test
    void shouldCreateNodesForAllModules() {
        GraphModel model = GraphModel.from(TestMavenModuleStructure.example());

        assertThat(model.getNodes())
                .extracting(GraphNode::toString)
                .containsExactly(
                        "core",
                        "configuration",
                        "ingest",
                        "bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter",
                        "distribution");
    }

    @Test
    void shouldCreateEdgesForInternalDependenciesWhichAreNotAlsoTransitive() {
        GraphModel model = GraphModel.from(TestMavenModuleStructure.example());

        assertThat(model.getEdges())
                .extracting(GraphEdge::toString)
                .containsExactly(
                        "configuration > core",
                        "ingest > configuration",
                        "bulk-import/bulk-import-core > configuration",
                        "bulk-import/bulk-import-runner > bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-runner > ingest",
                        "bulk-import/bulk-import-starter > bulk-import/bulk-import-core");
    }

    @Test
    void shouldCreateEdgesForTwoChainedInternalDependencies() {
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                sourceModuleBuilder("core").build(),
                sourceModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build(),
                sourceModuleBuilder("ingest").dependenciesArray(dependency("sleeper:configuration")).build()).build());

        assertThat(model.getEdges())
                .extracting(GraphEdge::toString)
                .containsExactly(
                        "configuration > core",
                        "ingest > configuration");
    }

    @Test
    void shouldNotCreateEdgeForInternalDependencyWhichIsAlsoTransitive() {
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                sourceModuleBuilder("core").build(),
                sourceModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build(),
                sourceModuleBuilder("ingest").dependenciesArray(
                        dependency("sleeper:configuration"),
                        dependency("sleeper:core")).build())
                .build());

        assertThat(model.getEdges())
                .extracting(GraphEdge::toString)
                .containsExactly(
                        "configuration > core",
                        "ingest > configuration");
    }

    @Test
    void shouldNotCreateEdgeForInternalDependencyWhichIsAlsoIndirectlyTransitive() {
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                sourceModuleBuilder("core").build(),
                sourceModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build(),
                sourceModuleBuilder("ingest").dependenciesArray(dependency("sleeper:configuration")).build(),
                sourceModuleBuilder("bulk-import").dependenciesArray(
                        dependency("sleeper:ingest"),
                        dependency("sleeper:core")).build())
                .build());

        assertThat(model.getEdges())
                .extracting(GraphEdge::toString)
                .containsExactly(
                        "configuration > core",
                        "ingest > configuration",
                        "bulk-import > ingest");
    }

    @Test
    void shouldNotCreateEdgeForInternalDependencyWhichDoesNotExist() {
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                sourceModuleBuilder("core").build(),
                sourceModuleBuilder("configuration").dependenciesArray(
                        dependency("sleeper:core"),
                        dependency("sleeper:not-a-module")).build())
                .build());

        assertThat(model.getEdges())
                .extracting(GraphEdge::toString)
                .containsExactly(
                        "configuration > core");
    }

    @Test
    void shouldFindEdgeByFromTo() {
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                sourceModuleBuilder("core").build(),
                sourceModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build(),
                sourceModuleBuilder("ingest").dependenciesArray(dependency("sleeper:configuration")).build()).build());

        assertThat(model.edgeByFromTo(moduleRef("configuration"), moduleRef("core")))
                .map(GraphEdge::toString)
                .contains("configuration > core");
        assertThat(model.edgeByFromTo(moduleRef("ingest"), moduleRef("configuration")))
                .map(GraphEdge::toString)
                .contains("ingest > configuration");
    }
}
