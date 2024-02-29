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

import sleeper.core.util.SplitIntoBatches;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class InvokeForTableRequest {

    private final List<String> tableIds;

    public InvokeForTableRequest(List<String> tableIds) {
        this.tableIds = tableIds;
    }

    public List<String> getTableIds() {
        return tableIds;
    }

    public static void sendForAllTables(TableIndex tableIndex, int batchSize, Consumer<InvokeForTableRequest> sendRequest) {
        sendForTables(tableIndex.streamAllTables(), batchSize, sendRequest);
    }

    public static void sendForTables(Stream<TableStatus> tables, int batchSize, Consumer<InvokeForTableRequest> sendRequest) {
        SplitIntoBatches.reusingListOfSize(batchSize,
                tables.map(TableStatus::getTableUniqueId),
                tableIds -> sendRequest.accept(new InvokeForTableRequest(tableIds)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableIds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof InvokeForTableRequest)) {
            return false;
        }
        InvokeForTableRequest other = (InvokeForTableRequest) obj;
        return Objects.equals(tableIds, other.tableIds);
    }

    @Override
    public String toString() {
        return "InvokeForTableRequest{tableIds=" + tableIds + "}";
    }

}
