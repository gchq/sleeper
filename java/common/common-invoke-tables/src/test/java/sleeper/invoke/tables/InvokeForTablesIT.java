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
package sleeper.invoke.tables;

import org.junit.jupiter.api.Test;

import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.table.TableStatusTestHelper.uniqueIdAndName;

public class InvokeForTablesIT extends LocalStackTestBase {

    @Test
    void shouldSendOneMessage() {
        // Given
        String queueUrl = createFifoQueueGetUrl();

        // When
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl, Stream.of(
                uniqueIdAndName("table-id", "table-name")));

        // Then
        assertThat(receiveMessages(queueUrl))
                .containsExactly("table-id");
    }

    @Test
    void shouldSendMoreMessagesThanFitInAnSqsSendMessageBatch() {
        // Given a FIFO queue
        String queueUrl = createFifoQueueGetUrl();

        // When we send more than the SQS hard limit of 10 messages to send in a single batch
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl,
                IntStream.rangeClosed(1, 11)
                        .mapToObj(i -> uniqueIdAndName("table-id-" + i, "table-name-" + i)));

        // Then we can receive those messages
        assertThat(receiveMessages(queueUrl)).containsExactly(
                "table-id-1", "table-id-2", "table-id-3", "table-id-4", "table-id-5",
                "table-id-6", "table-id-7", "table-id-8", "table-id-9", "table-id-10");
        assertThat(receiveMessages(queueUrl)).containsExactly(
                "table-id-11");
    }

    @Test
    void shouldLookUpTableByName() {
        // Given
        String queueUrl = createFifoQueueGetUrl();
        TableIndex tableIndex = new InMemoryTableIndex();
        tableIndex.create(uniqueIdAndName("table-id", "table-name"));

        // When
        InvokeForTables.sendOneMessagePerTableByName(sqsClient, queueUrl, tableIndex, List.of("table-name"));

        // Then
        assertThat(receiveMessages(queueUrl))
                .containsExactly("table-id");
    }

    @Test
    void shouldFailLookUpTableByName() {
        // Given
        String queueUrl = createFifoQueueGetUrl();
        TableIndex tableIndex = new InMemoryTableIndex();

        // When / Then
        assertThatThrownBy(() -> InvokeForTables.sendOneMessagePerTableByName(
                sqsClient, queueUrl, tableIndex, List.of("missing-table")))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(receiveMessages(queueUrl))
                .isEmpty();
    }

    @Test
    void shouldFailLookUpTableByNameOnSecondPage() {
        // Given
        String queueUrl = createFifoQueueGetUrl();
        TableIndex tableIndex = new InMemoryTableIndex();
        IntStream.rangeClosed(1, 11)
                .mapToObj(i -> uniqueIdAndName("table-id-" + i, "table-name-" + i))
                .forEach(tableIndex::create);

        // When / Then
        assertThatThrownBy(() -> InvokeForTables.sendOneMessagePerTableByName(sqsClient, queueUrl, tableIndex,
                IntStream.rangeClosed(1, 12)
                        .mapToObj(i -> "table-name-" + i)
                        .collect(toUnmodifiableList())))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(receiveMessages(queueUrl))
                .isEmpty();
    }

}
