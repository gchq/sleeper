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
package sleeper.bulkimport.job.runner;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

public class StructTypeFactoryTest {

    @Test
    public void shouldCreateCorrectStructType() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new IntType()), new Field("key2", new LongType()));
        schema.setSortKeyFields(new Field("sort1", new StringType()), new Field("sort2", new ByteArrayType()));
        schema.setValueFields(new Field("value1", new IntType()),
                new Field("value2", new LongType()),
                new Field("value3", new StringType()),
                new Field("value4", new ByteArrayType()),
                new Field("value5", new ListType(new LongType())),
                new Field("value6", new MapType(new IntType(), new StringType())));
        StructTypeFactory structTypeFactory = new StructTypeFactory();

        // When
        StructType structType = structTypeFactory.getStructType(schema);

        // Then
        assertEquals(10, structType.length());
        StructField key1 = StructField.apply("key1", DataTypes.IntegerType, false, Metadata.empty());
        assertEquals(key1, structType.fields()[0]);
        StructField key2 = StructField.apply("key2", DataTypes.LongType, false, Metadata.empty());
        assertEquals(key2, structType.fields()[1]);
        StructField sort1 = StructField.apply("sort1", DataTypes.StringType, false, Metadata.empty());
        assertEquals(sort1, structType.fields()[2]);
        StructField sort2 = StructField.apply("sort2", DataTypes.BinaryType, false, Metadata.empty());
        assertEquals(sort2, structType.fields()[3]);
        StructField value1 = StructField.apply("value1", DataTypes.IntegerType, false, Metadata.empty());
        assertEquals(value1, structType.fields()[4]);
        StructField value2 = StructField.apply("value2", DataTypes.LongType, false, Metadata.empty());
        assertEquals(value2, structType.fields()[5]);
        StructField value3 = StructField.apply("value3", DataTypes.StringType, false, Metadata.empty());
        assertEquals(value3, structType.fields()[6]);
        StructField value4 = StructField.apply("value4", DataTypes.BinaryType, false, Metadata.empty());
        assertEquals(value4, structType.fields()[7]);
        StructField value5 = StructField.apply("value5", DataTypes.createArrayType(DataTypes.LongType), false, Metadata.empty());
        assertEquals(value5, structType.fields()[8]);
        StructField value6 = StructField.apply("value6", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType), false, Metadata.empty());
        assertEquals(value6, structType.fields()[9]);
    }
}
