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

package sleeper.core.schema.utils;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static sleeper.core.schema.utils.ArrowConverter.convertArrowFieldToSleeperField;
import static sleeper.core.schema.utils.ArrowConverter.convertArrowSchemaToSleeperSchema;
import static sleeper.core.schema.utils.ArrowConverter.convertSleeperFieldToArrowField;
import static sleeper.core.schema.utils.ArrowConverter.convertSleeperSchemaToArrowSchema;

class ArrowConverterTest {
    private static final String FIELD_NAME = "test-field";

    private static Stream<Arguments> getSleeperFieldToArrowField() {
        return Stream.of(
                arguments(named("ByteArrayType", sleeperField(new ByteArrayType())),
                        named("ArrowType.Binary", arrowField(new ArrowType.Binary()))),
                arguments(named("IntType", sleeperField(new IntType())),
                        named("ArrowType.Int 32-bit", arrowField(new ArrowType.Int(32, true)))),
                arguments(named("LongType", sleeperField(new LongType())),
                        named("ArrowType.Int 64-bit", arrowField(new ArrowType.Int(64, true)))),
                arguments(named("StringType", sleeperField(new StringType())),
                        named("ArrowType.Utf8", arrowField(new ArrowType.Utf8()))),
                arguments(named("List of StringType", sleeperField(new ListType(new StringType()))),
                        named("ArrowType.List of ArrowType.Utf8", arrowListField(new ArrowType.Utf8()))),
                arguments(named("Map of StringType to IntType", sleeperField(new MapType(new StringType(), new IntType()))),
                        named("ArrowType.List of ArrowType.Struct", arrowMapField(new ArrowType.Utf8(), new ArrowType.Int(32, true))))
        );
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowField(ArrowType type) {
        return arrowField("field", type);
    }

    @ParameterizedTest
    @MethodSource("getSleeperFieldToArrowField")
    void shouldConvertSleeperFieldToArrowField(
            Field sleeperField, org.apache.arrow.vector.types.pojo.Field expectedArrowField) {

        // When
        org.apache.arrow.vector.types.pojo.Field converted = convertSleeperFieldToArrowField(sleeperField);

        // Then
        assertThat(converted).isEqualTo(expectedArrowField);
    }

    @ParameterizedTest
    @MethodSource("getSleeperFieldToArrowField")
    void shouldConvertArrowFieldToSleeperField(
            Field expectedSleeperField, org.apache.arrow.vector.types.pojo.Field arrowField) {

        Field converted = convertArrowFieldToSleeperField(arrowField);

        // Then
        assertThat(converted).isEqualTo(expectedSleeperField);
    }

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
    void shouldConvertArrowSchemaToSleeperSchema() {
        // Given
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields = List.of(
                arrowField("field1", new ArrowType.Utf8()),
                arrowField("field2", new ArrowType.Utf8()),
                arrowField("field3", new ArrowType.Utf8()));
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(arrowFields);


        // When
        Schema sleeperSchema = convertArrowSchemaToSleeperSchema(arrowSchema,
                List.of("field1"), List.of("field2"), List.of("field3"));

        // Then
        assertThat(sleeperSchema)
                .isEqualTo(
                        Schema.builder()
                                .rowKeyFields(sleeperField("field1", new StringType()))
                                .sortKeyFields(sleeperField("field2", new StringType()))
                                .valueFields(sleeperField("field3", new StringType()))
                                .build());
    }

    @Test
    void shouldFailToConvertArrowPrimitiveFieldThatIsNotSupportedBySleeper() {
        // Given
        org.apache.arrow.vector.types.pojo.Field arrowField = arrowField(FIELD_NAME, new ArrowType.Duration(TimeUnit.SECOND));

        // When/Then
        assertThatThrownBy(() -> convertArrowFieldToSleeperField(arrowField))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Arrow primitive type Duration(SECOND) is not supported by Sleeper");
    }

    @Test
    void shouldFailToConvertArrowUnsignedIntFieldToSleeperField() {
        // Given
        org.apache.arrow.vector.types.pojo.Field arrowField = arrowField(FIELD_NAME, new ArrowType.Int(32, false));

        // When/Then
        assertThatThrownBy(() -> convertArrowFieldToSleeperField(arrowField))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Arrow int type with bitWidth=32 and signed=false is not supported by Sleeper");
    }

    @Test
    void shouldFailToConvertArrowStructListFieldWithNonPrimitivesToSleeperField() {
        // Given
        org.apache.arrow.vector.types.pojo.Field arrowField = arrowMapField(new ArrowType.Utf8(),
                arrowStructField("test-struct",
                        arrowField(new ArrowType.Utf8()),
                        arrowField(new ArrowType.Int(32, true))).getType());

        // When/Then
        assertThatThrownBy(() -> convertArrowFieldToSleeperField(arrowField))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Arrow struct field contains non-primitive field types, which is not supported by Sleeper");
    }

    @Test
    void shouldFailToConvertArrowStructListFieldWithMoreThanTwoFieldsToSleeperField() {
        // Given
        org.apache.arrow.vector.types.pojo.Field arrowField = arrowListField(
                arrowStructField("test-struct",
                        arrowField(new ArrowType.Utf8()),
                        arrowField(new ArrowType.Utf8()),
                        arrowField(new ArrowType.Utf8())).getType());

        // When/Then
        assertThatThrownBy(() -> convertArrowFieldToSleeperField(arrowField))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Arrow struct field does not contain exactly two field elements, which is not supported by Sleeper");
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowMapField(ArrowType keyType, ArrowType valueType) {
        return arrowMapField("field", keyType, valueType);
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowMapField(String name, ArrowType keyType, ArrowType valueType) {
        return arrowListField(name,
                arrowStructField(name,
                        arrowField("key", keyType),
                        arrowField("value", valueType)));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowListField(ArrowType type) {
        return arrowListField("field", type);
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowListField(String name, ArrowType type) {
        return arrowListField(name, arrowField("element", type));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowListField(String name, org.apache.arrow.vector.types.pojo.Field... fields) {
        return new org.apache.arrow.vector.types.pojo.Field(name, FieldType.notNullable(new ArrowType.List()),
                List.of(fields));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowStructField(String name, org.apache.arrow.vector.types.pojo.Field... fields) {
        return new org.apache.arrow.vector.types.pojo.Field(
                name + "-key-value-struct",
                new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.Struct(), null),
                List.of(fields));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowField(String name, ArrowType type) {
        return org.apache.arrow.vector.types.pojo.Field.notNullable(name, type);
    }

    private static Field sleeperMapField(String name, PrimitiveType keyType, PrimitiveType valueType) {
        return sleeperField(name, new MapType(keyType, valueType));
    }

    private static Field sleeperListField(String name, PrimitiveType type) {
        return sleeperField(name, new ListType(type));
    }

    private static Field sleeperField(Type type) {
        return sleeperField("field", type);
    }

    private static Field sleeperField(String name, Type type) {
        return new Field(name, type);
    }
}
