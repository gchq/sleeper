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

package sleeper.arrow.schema;


import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.arrow.schema.ConverterTestHelper.arrowField;
import static sleeper.arrow.schema.ConverterTestHelper.sleeperField;

public class SchemaBackedByArrowTest {
    @Test
    void shouldCreateSchemaBackedByArrowFromSleeperSchema() {
        // Given
        Schema sleeperSchema = Schema.builder()
                .rowKeyFields(sleeperField("rowKeyField1", new StringType()))
                .sortKeyFields(sleeperField("sortKeyField1", new StringType()))
                .valueFields(sleeperField("valueField1", new StringType()))
                .build();

        // When
        SchemaBackedByArrow schemaBackedByArrow = SchemaBackedByArrow.fromSleeperSchema(sleeperSchema);

        // Then
        assertThat(schemaBackedByArrow.getArrowSchema())
                .isEqualTo(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                List.of(arrowField("rowKeyField1", new ArrowType.Utf8()),
                                        arrowField("sortKeyField1", new ArrowType.Utf8()),
                                        arrowField("valueField1", new ArrowType.Utf8()))
                        )
                );
    }

    @Test
    void shouldCreateSchemaBackedByArrowFromArrowSchema() {
        // Given
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(
                List.of(
                        arrowField("rowKeyField1", new ArrowType.Utf8()),
                        arrowField("sortKeyField1", new ArrowType.Utf8()),
                        arrowField("valueField1", new ArrowType.Utf8())
                )
        );

        // When
        SchemaBackedByArrow schemaBackedByArrow = SchemaBackedByArrow.fromArrowSchema(arrowSchema,
                List.of("rowKeyField1"),
                List.of("sortKeyField1"));

        // Then
        assertThat(schemaBackedByArrow.getArrowSchema())
                .isEqualTo(arrowSchema);
    }

    @Test
    void shouldFailToCreateSchemaBackedByArrowFromArrowSchemaWithNoValueFields() {
        // Given
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(
                List.of(
                        arrowField("rowKeyField1", new ArrowType.Utf8()),
                        arrowField("sortKeyField1", new ArrowType.Utf8())
                )
        );

        // When/Then
        assertThatThrownBy(() -> SchemaBackedByArrow.fromArrowSchema(arrowSchema,
                List.of("rowKeyField1"), List.of("sortKeyField1")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
