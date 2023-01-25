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
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.arrow.schema.ConverterTestHelper.arrowField;
import static sleeper.arrow.schema.ConverterTestHelper.arrowListField;
import static sleeper.arrow.schema.ConverterTestHelper.arrowMapField;
import static sleeper.arrow.schema.ConverterTestHelper.arrowStructField;
import static sleeper.arrow.schema.ConverterTestHelper.sleeperField;
import static sleeper.arrow.schema.ConverterTestHelper.sleeperListField;
import static sleeper.arrow.schema.ConverterTestHelper.sleeperMapField;
import static sleeper.arrow.schema.SchemaConverter.convertArrowSchemaToSleeperSchema;
import static sleeper.arrow.schema.SchemaConverter.convertSleeperSchemaToArrowSchema;

public class SchemaConverterTest {
    @Test
    void shouldConvertSleeperSchemaWithPrimitiveValueToArrowSchema() {
        // Given
        Schema sleeperSchema = Schema.builder()
                .rowKeyFields(sleeperField("key", new StringType()))
                .valueFields(sleeperField("value", new IntType()))
                .build();

        // When
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = convertSleeperSchemaToArrowSchema(sleeperSchema);

        // Then
        assertThat(arrowSchema.getFields())
                .containsExactly(
                        arrowField("key", new ArrowType.Utf8()),
                        arrowField("value", new ArrowType.Int(32, true)));
    }

    @Test
    void shouldConvertSleeperSchemaWithListValueToArrowSchema() {
        // Given
        Schema sleeperSchema = Schema.builder()
                .rowKeyFields(sleeperField("key", new StringType()))
                .valueFields(sleeperListField("value", new IntType()))
                .build();

        // When
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = convertSleeperSchemaToArrowSchema(sleeperSchema);

        // Then
        assertThat(arrowSchema.getFields())
                .containsExactly(
                        arrowField("key", new ArrowType.Utf8()),
                        arrowListField("value", new ArrowType.Int(32, true)));
    }

    @Test
    void shouldConvertSleeperSchemaWithMapValueToArrowSchema() {
        // Given
        Schema sleeperSchema = Schema.builder()
                .rowKeyFields(sleeperField("key", new StringType()))
                .valueFields(sleeperMapField("value", new StringType(), new IntType()))
                .build();

        // When
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = convertSleeperSchemaToArrowSchema(sleeperSchema);

        // Then
        assertThat(arrowSchema.getFields())
                .containsExactly(
                        arrowField("key", new ArrowType.Utf8()),
                        arrowMapField("value", new ArrowType.Utf8(), new ArrowType.Int(32, true)));
    }

    @Test
    @Disabled("TODO")
    void shouldConvertArrowSchemaWithPrimitiveValueFieldToSleeperSchema() {
        // Given
        List<Field> arrowFields = List.of(
                arrowField("rowKeyField1", new ArrowType.Utf8()),
                arrowField("sortKeyField1", new ArrowType.Utf8()),
                arrowField("valueField1", new ArrowType.Utf8()));
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(arrowFields);


        // When
        Schema sleeperSchema = convertArrowSchemaToSleeperSchema(arrowSchema,
                List.of("rowKeyField1"),
                List.of("sortKeyField1"),
                List.of("valueField1"));

        // Then
        assertThat(sleeperSchema)
                .isEqualTo(
                        Schema.builder()
                                .rowKeyFields(sleeperField("rowKeyField1", new StringType()))
                                .sortKeyFields(sleeperField("sortKeyField1", new StringType()))
                                .valueFields(sleeperField("valueField1", new StringType()))
                                .build());
    }

    @Test
    @Disabled("TODO")
    void shouldConvertArrowSchemaWithStructListValueFieldToSleeperSchema() {
        // Given
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields = List.of(
                arrowField("rowKeyField1", new ArrowType.Utf8()),
                arrowField("sortKeyField1", new ArrowType.Utf8()),
                arrowListField("valueField1",
                        arrowStructField("structField1",
                                arrowField("primitiveField1", new ArrowType.Utf8()),
                                arrowField("primitiveField2", new ArrowType.Int(32, true))))
        );
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(arrowFields);


        // When
        Schema sleeperSchema = convertArrowSchemaToSleeperSchema(arrowSchema,
                List.of("rowKeyField1"),
                List.of("sortKeyField1"),
                List.of("valueField1"));

        // Then
        assertThat(sleeperSchema)
                .isEqualTo(
                        Schema.builder()
                                .rowKeyFields(sleeperField("rowKeyField1", new StringType()))
                                .sortKeyFields(sleeperField("sortKeyField1", new StringType()))
                                .valueFields(sleeperField("valueField1", new MapType(new StringType(), new IntType())))
                                .build());
    }
}
