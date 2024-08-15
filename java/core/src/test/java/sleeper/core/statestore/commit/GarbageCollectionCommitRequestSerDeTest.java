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
package sleeper.core.statestore.commit;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GarbageCollectionCommitRequestSerDeTest {

    private GarbageCollectionCommitRequestSerDe serDe = new GarbageCollectionCommitRequestSerDe();

    @Test
    void shouldSerialiseGarbageCollectionCommitRequest() {
        // Given
        GarbageCollectionCommitRequest garbageCollectionCommitRequest = new GarbageCollectionCommitRequest("tableId",
                List.of("file1.parquet", "file2.parquet", "file3.parquet"));

        // When
        String json = serDe.toJsonPrettyPrint(garbageCollectionCommitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(garbageCollectionCommitRequest);
        Approvals.verify(json);
    }

    @Test
    void shouldFailToDeserialiseNonGarbageCollectionCommitRequest() {
        assertThatThrownBy(() -> serDe.fromJson("{\"type\": \"OTHER\", \"request\":{}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
