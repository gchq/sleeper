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
package sleeper.trino.utils;

import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class SleeperPageBlockUtilsTest {
    VariableWidthBlockBuilder blockBuilder;

    @BeforeEach
    void setUp() {
        blockBuilder = new VariableWidthBlockBuilder(null, 100, 10000);
    }

    @Test
    void shouldWriteBigIntToBlock() {
        // Given
        Long testValue = 998819991197253L;

        // When
        SleeperPageBlockUtils.writeElementToBuilder(blockBuilder, new ArrayType(BIGINT), testValue);

        // Then
        assertThat(blockBuilder.build().getLong(0, 0)).isEqualTo(testValue);
    }

    @Test
    void shouldWriteIntegerToBlock() {
        // Given
        int testValue = 434;

        // When
        SleeperPageBlockUtils.writeElementToBuilder(blockBuilder, new ArrayType(INTEGER), testValue);

        // Then
        assertThat(blockBuilder.build().getInt(0, 0)).isEqualTo(testValue);
    }

    @Test
    void shouldWriteStringToBlock() {
        // Given
        String testValue = "test-string";

        // When
        SleeperPageBlockUtils.writeElementToBuilder(blockBuilder, new ArrayType(VARCHAR), testValue);

        // Then
        assertThat(blockBuilder.build().getSlice(0, 0, testValue.length()).toStringUtf8())
                .isEqualTo(testValue);
    }
}
