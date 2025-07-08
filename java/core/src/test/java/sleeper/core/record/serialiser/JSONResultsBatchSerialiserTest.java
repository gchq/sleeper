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
package sleeper.core.record.serialiser;

import org.junit.jupiter.api.Test;

import sleeper.core.record.ResultsBatch;
import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class JSONResultsBatchSerialiserTest {

    @Test
    public void testWriteRead() {
        // Given
        String queryId = "query1";
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()), new Field("column2", new LongType()))
                .sortKeyFields(new Field("column3", new StringType()), new Field("column4", new ByteArrayType()))
                .valueFields(new Field("column5", new ByteArrayType()), new Field("column6", new ByteArrayType()))
                .build();
        List<SleeperRow> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SleeperRow record = new SleeperRow();
            record.put("column1", i);
            record.put("column2", i * 100L);
            record.put("column3", "abc" + i);
            record.put("column4", new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)});
            record.put("column5", new byte[]{(byte) (i + 4), (byte) (i + 5), (byte) (i + 6), (byte) (i + 7)});
            record.put("column6", new byte[]{(byte) (i + 8), (byte) (i + 9), (byte) (i + 10), (byte) (i + 11), (byte) (i + 12)});
            records.add(record);
        }
        ResultsBatch resultsBatch = new ResultsBatch(queryId, schema, records);
        JSONResultsBatchSerialiser serialiser = new JSONResultsBatchSerialiser();

        // When
        String serialised = serialiser.serialise(resultsBatch);
        ResultsBatch deserialised = serialiser.deserialise(serialised);

        // Then
        assertThat(deserialised).isEqualTo(resultsBatch);
    }
}
