/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class OnPushPathsDiffTest {

    @Test
    public void shouldReportNoDifference() {
        // Given
        List<String> expected = asList("a", "b");
        List<String> actual = asList("a", "b");

        // When
        OnPushPathsDiff diff = OnPushPathsDiff.fromExpectedAndActual(expected, actual);

        // Then
        assertThat(diff).isEqualTo(identical(expected, actual));
    }

    @Test
    public void shouldReportEntriesMissing() {
        // Given
        List<String> expected = asList("a", "b", "c");
        List<String> actual = singletonList("a");

        // When
        OnPushPathsDiff diff = OnPushPathsDiff.fromExpectedAndActual(expected, actual);

        // Then
        assertThat(diff).isEqualTo(builder(expected, actual).missingEntries(asList("b", "c")).build());
    }

    @Test
    public void shouldReportExtraEntries() {
        // Given
        List<String> expected = singletonList("a");
        List<String> actual = asList("a", "b", "c");

        // When
        OnPushPathsDiff diff = OnPushPathsDiff.fromExpectedAndActual(expected, actual);

        // Then
        assertThat(diff).isEqualTo(builder(expected, actual).extraEntries(asList("b", "c")).build());
    }

    private static OnPushPathsDiff identical(List<String> expected, List<String> actual) {
        return builder(expected, actual).build();
    }

    private static OnPushPathsDiff.Builder builder(List<String> expected, List<String> actual) {
        return OnPushPathsDiff.builder().expected(expected).actual(actual)
                .extraEntries(emptyList()).missingEntries(emptyList());
    }
}
