/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.compaction.job.creation;

import java.util.List;
import java.util.Objects;

public class TableBatch {
    List<String> tableNames;

    private TableBatch(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public static TableBatch batchWithTables(List<String> tableNames) {
        return new TableBatch(tableNames);
    }

    public static TableBatch batchWithTables(String... tableNames) {
        return batchWithTables(List.of(tableNames));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableBatch that = (TableBatch) o;
        return Objects.equals(tableNames, that.tableNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableNames);
    }

    @Override
    public String toString() {
        return "TableBatch{" +
                "tableNames=" + tableNames +
                '}';
    }
}
