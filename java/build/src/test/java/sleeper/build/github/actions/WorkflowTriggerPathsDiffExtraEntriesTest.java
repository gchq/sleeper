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

import sleeper.build.testutil.TestProjectStructure;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.github.actions.OnPushPathsDiffTestHelper.builder;
import static sleeper.build.github.actions.OnPushPathsDiffTestHelper.identical;

public class WorkflowTriggerPathsDiffExtraEntriesTest {

    @Test
    public void shouldReportExtraEntryUnderMavenProjectPath() {
        // Given
        List<String> expected = asList("java/module-a", "java/module-b");
        List<String> actual = asList("java/module-a", "java/module-b", "java/module-c");

        // When
        WorkflowTriggerPathsDiff diff = WorkflowTriggerPathsDiff.fromExpectedAndActual(
                TestProjectStructure.exampleWithMavenPath("java"), expected, actual);

        // Then
        assertThat(diff).isEqualTo(builder(expected, actual)
                .extraEntries(Collections.singletonList("java/module-c"))
                .build());
        assertThat(diff.isValid()).isFalse();
    }

    @Test
    public void shouldNotReportExtraEntryOutsideMavenProjectPath() {
        // Given
        List<String> expected = asList("java/module-a", "java/module-b");
        List<String> actual = asList("code-style/style.xml", "java/module-a", "java/module-b");

        // When
        WorkflowTriggerPathsDiff diff = WorkflowTriggerPathsDiff.fromExpectedAndActual(
                TestProjectStructure.exampleWithMavenPath("java"), expected, actual);

        // Then
        assertThat(diff).isEqualTo(identical(expected, actual));
        assertThat(diff.isValid()).isTrue();
    }
}
