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
package sleeper.query.runner.websocket;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.query.core.output.ResultsOutputLocation;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryWebSocketStatusMessageSerDeTest {

    QueryWebSocketStatusMessageSerDe serDe = new QueryWebSocketStatusMessageSerDe();

    @Test
    void shouldSerDeQueryWasSplitToSubqueriesMessage() {
        // Given
        QueryWebSocketStatusMessage message = QueryWebSocketStatusMessage.queryWasSplitToSubqueries("test-query", List.of("subquery-1", "subquery-2"));

        // When
        String json = serDe.toJsonPrettyPrint(message);
        QueryWebSocketStatusMessage found = serDe.fromJson(json);

        // Then
        assertThat(found).isEqualTo(message);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSerDeQueryCompletedMessage() {
        // Given
        QueryWebSocketStatusMessage message = QueryWebSocketStatusMessage.queryCompleted("test-query", 123,
                List.of(new ResultsOutputLocation("s3", "s3a://test-bucket/test-file.parquet")));

        // When
        String json = serDe.toJsonPrettyPrint(message);
        QueryWebSocketStatusMessage found = serDe.fromJson(json);

        // Then
        assertThat(found).isEqualTo(message);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSerDeQueryErrorMessage() {
        // Given
        QueryWebSocketStatusMessage message = QueryWebSocketStatusMessage.queryError(
                "test-query", "Something went wrong", 123,
                List.of(new ResultsOutputLocation("s3", "s3a://test-bucket/test-file.parquet")));

        // When
        String json = serDe.toJsonPrettyPrint(message);
        QueryWebSocketStatusMessage found = serDe.fromJson(json);

        // Then
        assertThat(found).isEqualTo(message);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

}
