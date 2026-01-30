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
package sleeper.bulkimport.runner.sketches;

import org.apache.datasketches.quantiles.ItemsUnion;
import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.sketches.Sketches;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithMultipleKeys;

public class SketchesBuilderTest {

    @Test
    void shouldUnionTwoSketchFilesTogether() {
        // Given
        Schema schema = createSchemaWithKey("key", new IntType());
        SketchesBuilder builder = new SketchesBuilder(schema);
        Sketches sketches = Sketches.from(schema);

        sketches.update(new Row(Map.of("key", 123)));
        sketches.update(new Row(Map.of("key", 456)));

        // When
        builder.add(sketches);

        // Then
        ItemsUnion<Object> keyUnion = builder.getFieldNameToUnion().get("key");
        assertThat(keyUnion.getResult().getMinValue()).isEqualTo(123L);
        assertThat(keyUnion.getResult().getMaxValue()).isEqualTo(456L);
    }

    @Test
    void shouldUnionSketchesWithDifferentKeys() {
        // Given
        Schema schema = createSchemaWithMultipleKeys("alpha", new IntType(), "beta", new IntType());
        SketchesBuilder builder = new SketchesBuilder(schema);
        Sketches sketches = Sketches.from(schema);

        sketches.update(new Row(Map.of("alpha", 12)));
        sketches.update(new Row(Map.of("alpha", 34)));
        sketches.update(new Row(Map.of("beta", 56)));
        sketches.update(new Row(Map.of("beta", 78)));

        // When
        builder.add(sketches);

        // Then
        ItemsUnion<Object> alphaUnion = builder.getFieldNameToUnion().get("alpha");
        assertThat(alphaUnion.getResult().getMinValue()).isEqualTo(12L);
        assertThat(alphaUnion.getResult().getMaxValue()).isEqualTo(34L);

        ItemsUnion<Object> betaUnion = builder.getFieldNameToUnion().get("beta");
        assertThat(betaUnion.getResult().getMinValue()).isEqualTo(56L);
        assertThat(betaUnion.getResult().getMaxValue()).isEqualTo(78L);

    }
}
