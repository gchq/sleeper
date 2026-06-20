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
package sleeper.restapi.addTable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AddTableResponseSerDeTest {

    private final AddTableResponseSerDe serDe = new AddTableResponseSerDe();

    @Nested
    @DisplayName("Serialise add table responses")
    class SerialiseResponses {
        @Test
        void shouldSerialiseResponseToJson() {
            String json = serDe.toJson(AddTableResponse.builder()
                    .tableId("table-id")
                    .tableName("table-name")
                    .build());

            assertThat(json).isEqualTo("{\"tableId\":\"table-id\",\"tableName\":\"table-name\"}");
        }
    }

    @Nested
    @DisplayName("Deserialise add table responses")
    class DeserialiseResponses {
        @Test
        void shouldDeserialiseResponseFromJson() {
            AddTableResponse response = serDe.fromJson(
                    "{\"tableId\":\"table-id\",\"tableName\":\"table-name\"}");

            assertThat(response).isEqualTo(AddTableResponse.builder()
                    .tableId("table-id")
                    .tableName("table-name")
                    .build());
        }
    }

    @Nested
    @DisplayName("Round trip add table responses")
    class RoundTripResponses {
        @Test
        void shouldRoundTripResponse() {
            // Given
            AddTableResponse response = AddTableResponse.builder()
                    .tableId("table-id")
                    .tableName("table-name")
                    .build();

            // When
            AddTableResponse deserialisedResponse = serDe.fromJson(serDe.toJson(response));

            // Then
            assertThat(deserialisedResponse).isEqualTo(response);
        }
    }
}
