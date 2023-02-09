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

package sleeper.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.util.ClientUtils.formatDescription;

class ClientUtilsTest {
    @Test
    void shouldFormatSingleLineString() {
        // Given
        String singleLineString = "Test string that can fit on one line";

        // When
        String formattedString = formatDescription(singleLineString);

        // Then
        assertThat(formattedString)
                .isEqualTo("# Test string that can fit on one line");
    }

    @Test
    void shouldFormatAndLineWrapDescription() {
        // Given
        String multiLineString = "Test string that cannot fit on one line, so needs one or more than one lines to fit it all on the screen";

        // When
        String formattedDescription = formatDescription(multiLineString);

        // Then
        assertThat(formattedDescription)
                .isEqualTo("" +
                        "# Test string that cannot fit on one line, so needs one or more than one lines to fit it all on the\n" +
                        "# screen");
    }

    @Test
    void shouldFormatAndLineWrapDescriptionWithCustomLineBreaks() {
        // Given
        String multiLineString = "Test string that cannot fit on one line\nbut with a custom line break. " +
                "This is to verify if the line still wraps even after after a custom line break";

        // When
        String formattedDescription = formatDescription(multiLineString);

        // Then
        assertThat(formattedDescription)
                .isEqualTo("" +
                        "# Test string that cannot fit on one line\n" +
                        "# but with a custom line break. This is to verify if the line still wraps even after after a custom\n" +
                        "# line break");
    }
}
