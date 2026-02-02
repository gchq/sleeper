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
package sleeper.sketches;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SketchesUnionBuilderTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = createSchemaWithKey("key", new IntType());
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @Test
    void shouldBuildWhenProvidedWithOnlyOneSketch() {
        // Given
        SketchesUnionBuilder builder = new SketchesUnionBuilder(schema);
        Sketches sketches = Sketches.from(schema);
        sketches.update(new Row(Map.of("key", 12)));
        sketches.update(new Row(Map.of("key", 34)));
        sketches.update(new Row(Map.of("key", 56)));

        // When
        builder.add(sketches);
        // Then
        assertThat(SketchesDeciles.from(builder.build()))
                .isEqualTo(SketchesDeciles.from(tableProperties, List.of(
                        new Row(Map.of("key", 12)),
                        new Row(Map.of("key", 34)),
                        new Row(Map.of("key", 56)))));
    }

    @Test
    void shouldUnionTwoSketchesTogether() {
        // Given
        SketchesUnionBuilder builder = new SketchesUnionBuilder(schema);
        Sketches firstSketches = Sketches.from(schema);
        firstSketches.update(new Row(Map.of("key", 123)));

        Sketches secondSketches = Sketches.from(schema);
        secondSketches.update(new Row(Map.of("key", 456)));

        // When
        builder.add(firstSketches);
        builder.add(secondSketches);

        // Then
        assertThat(SketchesDeciles.from(builder.build()))
                .isEqualTo(SketchesDeciles.from(tableProperties, List.of(
                        new Row(Map.of("key", 123)),
                        new Row(Map.of("key", 456)))));
    }
}
