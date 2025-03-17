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

package sleeper.core.table;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TableIdGeneratorTest {

    @Test
    void shouldGenerate32BitHexId() {
        assertThat(TableIdGenerator.fromRandomSeed(0).generateString())
                .isEqualTo("60b420bb");
    }

    @Test
    void shouldGenerateAnother32BitHexId() {
        assertThat(TableIdGenerator.fromRandomSeed(123).generateString())
                .isEqualTo("ddf121b9");
    }

    @Test
    void shouldGenerateDifferentIdsFromSameGenerator() {
        TableIdGenerator generator = TableIdGenerator.fromRandomSeed(42);
        assertThat(List.of(generator.generateString(), generator.generateString()))
                .containsExactly("359d41ba", "f78afe0d");
    }
}
