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
package sleeper.core.schema;

import org.junit.jupiter.api.Test;

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaTest {

    private static Schema.Builder equalsTestBuilder() {
        return Schema.builder()
                .rowKeyFields(
                        new Field("column1", new IntType()),
                        new Field("column2", new StringType()))
                .sortKeyFields(
                        new Field("column3", new IntType()))
                .valueFields(
                        new Field("column4", new MapType(new IntType(), new ByteArrayType())),
                        new Field("column5", new ListType(new StringType())));
    }

    @Test
    public void equalsShouldReturnCorrectResults() {
        // Given
        Schema schema1 = equalsTestBuilder().build();
        Schema schema2 = equalsTestBuilder().build();
        Schema schema3 = equalsTestBuilder()
                .valueFields(new Field("column4", new MapType(new IntType(), new StringType())))
                .build();

        // When
        boolean test1 = schema1.equals(schema2);
        boolean test2 = schema1.equals(schema3);

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
    }

    @Test
    public void shouldntAllowMapTypeAsRowKey() {
        // Given
        Field allowField = new Field("column1", new IntType());
        Field refuseField = new Field("column2", new MapType(new IntType(), new ByteArrayType()));
        Schema.Builder builder = Schema.builder().rowKeyFields(allowField, refuseField);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Row key", "type");
    }

    @Test
    public void shouldntAllowMapTypeAsSortKey() {
        // Given
        List<Field> rowKeyFields = Arrays.asList(
                new Field("column1", new IntType()),
                new Field("column2", new StringType()));
        Field refuseField = new Field("column3", new MapType(new IntType(), new ByteArrayType()));
        Schema.Builder builder = Schema.builder().rowKeyFields(rowKeyFields).sortKeyFields(refuseField);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Sort key", "type");
    }

    @Test
    public void shouldntAllowListTypeAsRowKey() {
        // Given
        Field allowField = new Field("column1", new IntType());
        Field refuseField = new Field("column2", new ListType(new IntType()));
        Schema.Builder builder = Schema.builder().rowKeyFields(allowField, refuseField);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Row key", "type");
    }

    @Test
    public void shouldntAllowListTypeAsSortKey() {
        // Given
        List<Field> rowKeyFields = Arrays.asList(
                new Field("column1", new IntType()),
                new Field("column2", new StringType()));
        Field refuseField = new Field("column3", new ListType(new IntType()));
        Schema.Builder builder = Schema.builder().rowKeyFields(rowKeyFields).sortKeyFields(refuseField);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Sort key", "type");
    }

    @Test
    public void refuseNoKeyFields() {
        // Given
        Schema.Builder builder = Schema.builder()
                .rowKeyFields()
                .sortKeyFields(new Field("column1", new IntType()))
                .valueFields(new Field("column2", new StringType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void refuseNullKeyFields() {
        // Given
        Schema.Builder builder = Schema.builder()
                .rowKeyFields((List<Field>) null)
                .sortKeyFields(new Field("column1", new IntType()))
                .valueFields(new Field("column2", new StringType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void setNoSortKeysByDefault() {
        // Given
        Field rowKeyField = new Field("column1", new IntType());
        Field valueField = new Field("column2", new StringType());

        // When / Then
        assertThat(Schema.builder()
                .rowKeyFields(rowKeyField).valueFields(valueField)
                .build())
                .isEqualTo(Schema.builder()
                        .rowKeyFields(rowKeyField).valueFields(valueField)
                        .sortKeyFields(Collections.emptyList())
                        .build());
    }

    @Test
    public void setNoValueFieldsByDefault() {
        // Given
        Field rowKeyField = new Field("column1", new IntType());
        Field sortKeyField = new Field("column2", new StringType());

        // When / Then
        assertThat(Schema.builder()
                .rowKeyFields(rowKeyField).sortKeyFields(sortKeyField)
                .build())
                .isEqualTo(Schema.builder()
                        .rowKeyFields(rowKeyField).sortKeyFields(sortKeyField)
                        .valueFields(Collections.emptyList())
                        .build());
    }

    @Test
    public void setNoSortAndValueFieldsByDefault() {
        // Given
        Field rowKeyField = new Field("column1", new IntType());

        // When / Then
        assertThat(Schema.builder()
                .rowKeyFields(rowKeyField)
                .build())
                .isEqualTo(Schema.builder()
                        .rowKeyFields(rowKeyField)
                        .sortKeyFields(Collections.emptyList())
                        .valueFields(Collections.emptyList())
                        .build());
    }
}
