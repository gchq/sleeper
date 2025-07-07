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
package sleeper.trino.testutils;

import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import sleeper.trino.utils.SleeperPageBlockUtils;

import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class SleeperBlockUtilsIT {

    @Test
    void shouldWriteElementsToBlockBuilder() {
        // Given
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 100, 10000);
        Map<ArrayType, String> valueMap = Map.of(
                new ArrayType(BIGINT), "01189998819991197253",
                new ArrayType(INTEGER), "464",
                new ArrayType(VARCHAR), "Words");
        // When
        valueMap.forEach((key, value) -> SleeperPageBlockUtils.writeElementToBuilder(blockBuilder, (ArrayType) key, value));

        // Then
        // Build the block containing the written details and extract the first slice
        // Converts back to a string for validation that contains the string
        String assertionString = new String(blockBuilder.build().getSlice(0, 0, 1).byteArray());
        valueMap.keySet().forEach(key -> {
            assertThat(assertionString).containsOnlyOnce(valueMap.get(key));
        });
    }

}
