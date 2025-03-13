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
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaDuplicateFieldsTest {

    @Test
    public void refuseRowKeysWithSameName() {
        // Given
        Schema.Builder builder = Schema.builder().rowKeyFields(
                new Field("column1", new IntType()),
                new Field("column1", new StringType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column1");
    }

    @Test
    public void refuseSortKeysWithSameName() {
        // Given
        Schema.Builder builder = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()))
                .sortKeyFields(
                        new Field("column2", new StringType()),
                        new Field("column2", new ByteArrayType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column2");
    }

    @Test
    public void refuseValueFieldsWithSameName() {
        // Given
        Schema.Builder builder = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()))
                .valueFields(
                        new Field("column2", new StringType()),
                        new Field("column2", new ByteArrayType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column2");
    }

    @Test
    public void refuseFieldNameInRowKeysAndSortKeys() {
        // Given
        Schema.Builder builder = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()))
                .sortKeyFields(new Field("column1", new StringType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column1");
    }

    @Test
    public void refuseFieldNameInSortKeysAndValueKeys() {
        // Given
        Schema.Builder builder = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()))
                .sortKeyFields(new Field("column2", new StringType()))
                .valueFields(new Field("column2", new ByteArrayType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column2");
    }

    @Test
    public void refuseFieldNameInRowKeysAndValueKeys() {
        // Given
        Schema.Builder builder = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()))
                .sortKeyFields(new Field("column2", new StringType()))
                .valueFields(new Field("column1", new ByteArrayType()));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column1");
    }
}
