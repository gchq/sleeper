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
package sleeper.build.github.actions;

import org.junit.jupiter.api.Test;

import sleeper.build.chunks.ProjectStructure;
import sleeper.build.testutil.TestProjectStructure;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.github.actions.OnPushPathsDiffTestHelper.builder;
import static sleeper.build.github.actions.OnPushPathsDiffTestHelper.identical;

public class WorkflowTriggerPathsDiffTest {

    private final ProjectStructure project = TestProjectStructure.example();

    private WorkflowTriggerPathsDiff fromExpectedAndActual(List<String> expected, List<String> actual) {
        return WorkflowTriggerPathsDiff.fromExpectedAndActual(project, expected, actual);
    }

    @Test
    public void shouldReportNoDifference() {
        // Given
        List<String> expected = asList("a", "b");
        List<String> actual = asList("a", "b");

        // When
        WorkflowTriggerPathsDiff diff = fromExpectedAndActual(expected, actual);

        // Then
        assertThat(diff).isEqualTo(identical(expected, actual));
        assertThat(diff.isValid()).isTrue();
    }

    @Test
    public void shouldReportEntriesMissing() {
        // Given
        List<String> expected = asList("a", "b", "c");
        List<String> actual = singletonList("a");

        // When
        WorkflowTriggerPathsDiff diff = fromExpectedAndActual(expected, actual);

        // Then
        assertThat(diff).isEqualTo(builder(expected, actual).missingEntries(asList("b", "c")).build());
        assertThat(diff.isValid()).isFalse();
    }
}
