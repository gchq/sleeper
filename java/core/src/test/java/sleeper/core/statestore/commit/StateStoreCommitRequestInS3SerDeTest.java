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

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitRequestInS3SerDeTest {
    private final StateStoreCommitRequestInS3SerDe serDe = new StateStoreCommitRequestInS3SerDe();

    @Test
    void shouldSerialiseCommitRequestInS3() {
        // Given
        StateStoreCommitRequestInS3 commitRequest = new StateStoreCommitRequestInS3("test-data-bucket/test-commit-request.json");

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json);
    }
}
