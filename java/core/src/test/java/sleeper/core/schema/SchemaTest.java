/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaTest {

    @Test
    public void equalsShouldReturnCorrectResults() {
        // Given
        Schema schema1 = new Schema();
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
        schema1.setSortKeyFields(new Field("column3", new IntType()));
        schema1.setValueFields(new Field("column4", new MapType(new IntType(), new ByteArrayType())),
                new Field("column5", new ListType(new StringType())));
        Schema schema2 = new Schema();
        schema2.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
        schema2.setSortKeyFields(new Field("column3", new IntType()));
        schema2.setValueFields(new Field("column4", new MapType(new IntType(), new ByteArrayType())),
                new Field("column5", new ListType(new StringType())));
        Schema schema3 = new Schema();
        schema3.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
        schema3.setSortKeyFields(new Field("column3", new IntType()));
        schema3.setValueFields(new Field("column4", new MapType(new IntType(), new StringType())));

        // When
        boolean test1 = schema1.equals(schema2);
        boolean test2 = schema1.equals(schema3);

        // Then
        assertTrue(test1);
        assertFalse(test2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldntAllowMapTypeAsRowKey() {
        // Given
        Schema schema1 = new Schema();
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new MapType(new IntType(), new ByteArrayType())));

        // When / Then
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldntAllowMapTypeAsSortKey() {
        // Given
        Schema schema1 = new Schema();
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
        schema1.setSortKeyFields(new Field("column3", new MapType(new IntType(), new ByteArrayType())));

        // When / Then
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldntAllowListTypeAsRowKey() {
        // Given
        Schema schema1 = new Schema();
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new ListType(new IntType())));

        // When / Then
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldntAllowListTypeAsSortKey() {
        // Given
        Schema schema1 = new Schema();
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
        schema1.setSortKeyFields(new Field("column3", new ListType(new IntType())));

        // When / Then
        schema1.setRowKeyFields(new Field("column1", new IntType()), new Field("column2", new StringType()));
    }
}
