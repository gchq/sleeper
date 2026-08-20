/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.parquet.row;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaConverterCompatibilityTest {

    private final Schema tableSchema = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .sortKeyFields(new Field("timestamp", new LongType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    private static Types.MessageTypeBuilder message() {
        return Types.buildMessage();
    }

    @Test
    void shouldBeCompatibleWhenSchemaMatchesExactly() {
        MessageType fileSchema = message()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .required(PrimitiveTypeName.INT64).named("timestamp")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("record");

        assertThat(SchemaConverter.isFileSchemaCompatibleWithTable(fileSchema, tableSchema)).isTrue();
    }

    @Test
    void shouldNotBeCompatibleWhenAFieldNameDiffers() {
        MessageType fileSchema = message()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("id")
                .required(PrimitiveTypeName.INT64).named("timestamp")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("record");

        assertThat(SchemaConverter.isFileSchemaCompatibleWithTable(fileSchema, tableSchema)).isFalse();
    }

    @Test
    void shouldNotBeCompatibleWhenAFieldTypeDiffers() {
        MessageType fileSchema = message()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .required(PrimitiveTypeName.INT32).named("timestamp")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("record");

        assertThat(SchemaConverter.isFileSchemaCompatibleWithTable(fileSchema, tableSchema)).isFalse();
    }

    @Test
    void shouldBeCompatibleWhenFileHasExtraColumns() {
        MessageType fileSchema = message()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .required(PrimitiveTypeName.INT64).named("timestamp")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .required(PrimitiveTypeName.INT64).named("extra")
                .named("record");

        assertThat(SchemaConverter.isFileSchemaCompatibleWithTable(fileSchema, tableSchema)).isTrue();
    }

    @Test
    void shouldBeCompatibleWhenColumnsAreInADifferentOrder() {
        MessageType fileSchema = message()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .required(PrimitiveTypeName.INT64).named("timestamp")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .named("record");

        assertThat(SchemaConverter.isFileSchemaCompatibleWithTable(fileSchema, tableSchema)).isTrue();
    }

    @Test
    void shouldBeCompatibleWhenNullabilityDiffers() {
        MessageType fileSchema = message()
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .optional(PrimitiveTypeName.INT64).named("timestamp")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("record");

        assertThat(SchemaConverter.isFileSchemaCompatibleWithTable(fileSchema, tableSchema)).isTrue();
    }

    @Test
    void shouldNotBeCompatibleWhenATableFieldIsMissingFromTheFile() {
        MessageType fileSchema = message()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .required(PrimitiveTypeName.INT64).named("timestamp")
                .named("record");

        assertThat(SchemaConverter.isFileSchemaCompatibleWithTable(fileSchema, tableSchema)).isFalse();
    }

}
