/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UncheckedAutoCloseablesTest {

    @Test
    void shouldCloseMultiple() {
        // Given
        List<String> closed = new ArrayList<>();
        UncheckedAutoCloseable close1 = () -> closed.add("closed 1");
        UncheckedAutoCloseable close2 = () -> closed.add("closed 2");
        UncheckedAutoCloseables closeables = new UncheckedAutoCloseables(List.of(close1, close2));

        // When
        closeables.close();

        // Then
        assertThat(closed).containsExactly("closed 1", "closed 2");
    }

    @Test
    void shouldCloseAllWhenAllFail() {
        // Given
        UncheckedAutoCloseable close1 = () -> {
            throw new RuntimeException("Failed 1");
        };
        UncheckedAutoCloseable close2 = () -> {
            throw new RuntimeException("Failed 2");
        };
        UncheckedAutoCloseables closeables = new UncheckedAutoCloseables(List.of(close1, close2));

        // When / Then
        assertThatThrownBy(() -> closeables.close())
                .hasMessage("Failed to close 2 resource(s)")
                .isInstanceOfSatisfying(FailedCloseException.class,
                        e -> assertThat(e.getFailures())
                                .extracting(RuntimeException::getMessage)
                                .containsExactly("Failed 1", "Failed 2"));
    }

    @Test
    void shouldFailOneCloseOne() {
        // Given
        List<String> closed = new ArrayList<>();
        UncheckedAutoCloseable close1 = () -> {
            throw new RuntimeException("Failed 1");
        };
        UncheckedAutoCloseable close2 = () -> closed.add("closed 2");
        UncheckedAutoCloseables closeables = new UncheckedAutoCloseables(List.of(close1, close2));

        // When / Then
        assertThatThrownBy(() -> closeables.close())
                .hasMessage("Failed to close 1 resource(s)")
                .isInstanceOf(FailedCloseException.class)
                .cause()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed 1");
        assertThat(closed).containsExactly("closed 2");
    }

}
