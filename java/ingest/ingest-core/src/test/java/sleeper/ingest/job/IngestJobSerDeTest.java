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
package sleeper.ingest.job;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IngestJobSerDeTest {

    @Test
    void shouldSerDeCorrectly() {
        // Given
        IngestJob ingestJob = IngestJob.builder()
                .tableName("table").id("id").files(List.of("file1", "file2"))
                .build();
        IngestJobSerDe ingestJobSerDe = new IngestJobSerDe();

        // When
        IngestJob deserialisedJob = ingestJobSerDe.fromJson(ingestJobSerDe.toJson(ingestJob));

        // Then
        assertThat(deserialisedJob).isEqualTo(ingestJob);
    }
}
