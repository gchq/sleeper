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
package sleeper.core.table;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class InvokeForTableRequestTest {

    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();

    @Test
    void shouldSendRequestForTwoTables() {
        List<String> sent = new ArrayList<>();
        InvokeForTableRequest.sendForTables(
                Stream.of(table("table-1"), table("table-2")),
                1, request -> sent.add(serDe.toJson(request)));
        assertThat(sent).extracting(serDe::fromJson).containsExactly(
                new InvokeForTableRequest(List.of("table-1")),
                new InvokeForTableRequest(List.of("table-2")));
    }

    private TableStatus table(String tableName) {
        return TableStatusTestHelper.uniqueIdAndName(tableName, tableName);
    }
}
