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

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.core.schema.Field;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static sleeper.arrow.schema.ArrowFieldConverter.convertArrowFieldToSleeperField;
import static sleeper.arrow.schema.ArrowFieldConverter.convertSleeperFieldToArrowField;
import static sleeper.arrow.schema.ConverterTestHelper.arrowField;
import static sleeper.arrow.schema.ConverterTestHelper.arrowListField;
import static sleeper.arrow.schema.ConverterTestHelper.arrowMapField;
import static sleeper.arrow.schema.ConverterTestHelper.arrowStructField;
import static sleeper.arrow.schema.ConverterTestHelper.sleeperField;

class ArrowFieldConverterTest {
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
}
