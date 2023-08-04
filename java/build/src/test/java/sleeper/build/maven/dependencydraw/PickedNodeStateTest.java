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

import java.awt.Color;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.maven.TestMavenModuleStructure.dependency;
import static sleeper.build.maven.TestMavenModuleStructure.moduleRef;
import static sleeper.build.maven.TestMavenModuleStructure.rootBuilder;
import static sleeper.build.maven.TestMavenModuleStructure.testedModuleBuilder;

public class PickedNodeStateTest {

    @Test
    void shouldDrawUnpickedDependencyInBlack() {
        // Given
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                testedModuleBuilder("core").build(),
                testedModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build()
        ).build());

        // When
        PickedNodeState picked = new PickedNodeState(model);

        // Then
        GraphEdge edge = model.getEdgeByFromTo(moduleRef("configuration"), moduleRef("core"));
        assertThat(picked.calculateEdgeColor(edge, false))
                .isSameAs(Color.BLACK);
    }

    @Test
    void shouldDrawPickedDependencyInBlue() {
        // Given
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                testedModuleBuilder("core").build(),
                testedModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build()
        ).build());
        PickedNodeState picked = new PickedNodeState(model);

        // When
        picked.updatePicked(List.of(model.getNode(moduleRef("configuration"))));

        // Then
        GraphEdge edge = model.getEdgeByFromTo(moduleRef("configuration"), moduleRef("core"));
        assertThat(picked.calculateEdgeColor(edge, false))
                .isSameAs(Color.BLUE);
    }

    @Test
    void shouldDrawPickedDependentInRed() {
        // Given
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                testedModuleBuilder("core").build(),
                testedModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build()
        ).build());
        PickedNodeState picked = new PickedNodeState(model);

        // When
        picked.updatePicked(List.of(model.getNode(moduleRef("core"))));

        // Then
        GraphEdge edge = model.getEdgeByFromTo(moduleRef("configuration"), moduleRef("core"));
        assertThat(picked.calculateEdgeColor(edge, false))
                .isSameAs(Color.RED);
    }

    @Test
    void shouldDrawUnpickedDependencyInGreyWhenViewingTransitives() {
        // Given
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                testedModuleBuilder("core").build(),
                testedModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build(),
                testedModuleBuilder("ingest").dependenciesArray(dependency("sleeper:configuration")).build(),
                testedModuleBuilder("splitter").dependenciesArray(dependency("sleeper:configuration")).build()
        ).build());
        PickedNodeState picked = new PickedNodeState(model);

        // When
        picked.updatePicked(List.of(model.getNode(moduleRef("ingest"))));

        // Then
        GraphEdge edge = model.getEdgeByFromTo(moduleRef("splitter"), moduleRef("configuration"));
        assertThat(picked.calculateEdgeColor(edge, true))
                .isSameAs(Color.lightGray);
    }

    @Test
    void shouldDrawIndirectTransitiveInBlack() {
        // Given
        GraphModel model = GraphModel.from(rootBuilder().modulesArray(
                testedModuleBuilder("core").build(),
                testedModuleBuilder("configuration").dependenciesArray(dependency("sleeper:core")).build(),
                testedModuleBuilder("ingest").dependenciesArray(dependency("sleeper:configuration")).build(),
                testedModuleBuilder("bulk-import").dependenciesArray(dependency("sleeper:ingest")).build()
        ).build());
        PickedNodeState picked = new PickedNodeState(model);

        // When
        picked.updatePicked(List.of(model.getNode(moduleRef("bulk-import"))));

        // Then
        GraphEdge edge = model.getEdgeByFromTo(moduleRef("configuration"), moduleRef("core"));
        assertThat(picked.calculateEdgeColor(edge, true))
                .isSameAs(Color.BLACK);
    }
}
