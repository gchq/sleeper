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
package sleeper.core.row.serialiser;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class RowSerialiserTest {

    @Test
    public void shouldSerialiseAndDeserialiseCorrectly() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()), new Field("column2", new LongType()))
                .sortKeyFields(new Field("column3", new StringType()), new Field("column4", new ByteArrayType()))
                .valueFields(new Field("column5", new ByteArrayType()), new Field("column6", new ByteArrayType()))
                .build();
        Row row = new Row();
        row.put("column1", 19);
        row.put("column2", 100L);
        row.put("column3", "abc");
        row.put("column4", new byte[]{1, 2, 3});
        row.put("column5", new byte[]{4, 5, 6, 7});
        row.put("column6", new byte[]{8, 9, 10, 11, 12});
        RowSerialiser serialiser = new RowSerialiser(schema);

        // When
        Row deserialised = serialiser.deserialise(serialiser.serialise(row));

        // Then
        assertThat(deserialised).isEqualTo(row);
    }
}
