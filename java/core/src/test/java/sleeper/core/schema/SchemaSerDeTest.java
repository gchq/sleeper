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
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import static org.junit.Assert.assertEquals;

public class SchemaSerDeTest {

    @Test
    public void shouldSerDeCorrectly() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new IntType()), new Field("column1", new LongType()));
        schema.setSortKeyFields(new Field("column3", new StringType()), new Field("column4", new ByteArrayType()));
        schema.setValueFields(
                new Field("column5", new MapType(new IntType(), new StringType())),
                new Field("column6", new ByteArrayType()),
                new Field("column7", new ListType(new StringType()))
        );
        SchemaSerDe schemaSerDe = new SchemaSerDe();

        // When
        Schema read = schemaSerDe.fromJson(schemaSerDe.toJson(schema));

        // Then
        assertEquals(schema, read);
    }

    @Test
    public void shouldDeserialiseFromJsonString() {
        // Given
        String jsonSchema = "{\n" +
                "  \"rowKeyFields\": [\n" +
                "    {\n" +
                "      \"name\": \"column1\",\n" +
                "      \"type\": \"IntType\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"column1\",\n" +
                "      \"type\": \"LongType\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"sortKeyFields\": [\n" +
                "    {\n" +
                "      \"name\": \"column3\",\n" +
                "      \"type\": \"StringType\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"column4\",\n" +
                "      \"type\": \"ByteArrayType\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"valueFields\": [\n" +
                "    {\n" +
                "      \"name\": \"column5\",\n" +
                "      \"type\": {\n" +
                "        \"MapType\": {\n" +
                "          \"keyType\": \"IntType\",\n" +
                "          \"valueType\": \"StringType\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"column6\",\n" +
                "      \"type\": \"ByteArrayType\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"column7\",\n" +
                "      \"type\": {\n" +
                "        \"ListType\": {\n" +
                "          \"elementType\": \"StringType\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";
        SchemaSerDe schemaSerDe = new SchemaSerDe();

        // When
        Schema deserialisedSchema = schemaSerDe.fromJson(jsonSchema);

        // Then
        Schema expectedSchema = new Schema();
        expectedSchema.setRowKeyFields(new Field("column1", new IntType()), new Field("column1", new LongType()));
        expectedSchema.setSortKeyFields(new Field("column3", new StringType()), new Field("column4", new ByteArrayType()));
        expectedSchema.setValueFields(
                new Field("column5", new MapType(new IntType(), new StringType())),
                new Field("column6", new ByteArrayType()),
                new Field("column7", new ListType(new StringType()))
        );
        assertEquals(expectedSchema, deserialisedSchema);
    }
}
