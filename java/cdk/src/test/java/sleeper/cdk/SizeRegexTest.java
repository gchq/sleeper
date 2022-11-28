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
package sleeper.cdk;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.cdk.stack.CompactionStack.normaliseSize;

public class SizeRegexTest {

    @Test
    public void shouldReturnNull() {
        assertThat(normaliseSize(null)).isNull();
    }

    @Test
    public void shouldReturnEmpty() {
        assertThat(normaliseSize("")).isEmpty();
    }

    @Test
    public void shouldNotAlterLettersOnly() {
        assertThat(normaliseSize("test")).isEqualTo("test");
    }

    @Test
    public void shouldReturnNumbersOnly() {
        assertThat(normaliseSize("1234567")).isEqualTo("1234567");
    }

    @Test
    public void shouldSwapSingleDigitToEnd() {
        assertThat(normaliseSize("2xlarge")).isEqualTo("xlarge2");
    }

    @Test
    public void shouldSwapMultipleDigitsToEnd() {
        assertThat(normaliseSize("56xlarge")).isEqualTo("xlarge56");
    }
}
