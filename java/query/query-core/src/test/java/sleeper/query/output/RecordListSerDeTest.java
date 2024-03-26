/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.query.output;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordListSerDeTest {
    @Test
    void shouldSerialiseRecordListToString() {
        // Given
        List<Record> recordList = List.of(new Record(Map.of("key1", "abc", "key2", 123L)));

        // When / Then
        assertThat(RecordListSerDe.toJson(recordList))
                .isEqualTo("[{\"values\":{\"key1\":\"abc\",\"key2\":123}}]");
    }

    @Test
    void shouldDeserialiseStringToRecordList() {
        // Given
        String recordJson = "[{\"values\":{\"key1\":\"abc\",\"key2\":123}}]";

        // When / Then
        assertThat(RecordListSerDe.fromJson(recordJson))
                .isEqualTo(List.of(new Record(Map.of("key1", "abc", "key2", 123L))));
    }

    @Test
    void shouldDeserialiseJsonElementToRecordList() {
        // Given
        JsonArray recordList = new JsonArray();
        JsonObject record = new JsonObject();
        JsonObject values = new JsonObject();
        values.addProperty("key1", "abc");
        values.addProperty("key2", 123L);
        record.add("values", values);
        recordList.add(record);

        // When / Then
        assertThat(RecordListSerDe.fromJson(recordList))
                .isEqualTo(List.of(new Record(Map.of("key1", "abc", "key2", 123L))));
    }
}
