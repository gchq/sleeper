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
package sleeper.api.auth;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * The tables a table-scoped permission has been granted over. Either every table in the instance, including tables
 * added later, or a fixed set of them.
 */
public class TableScope {

    private static final TableScope ALL_TABLES = new TableScope(Set.of());

    private final Set<String> tableIds;

    private TableScope(Set<String> tableIds) {
        this.tableIds = tableIds;
    }

    /**
     * Creates a scope covering every table in the instance, including any added later.
     *
     * @return the scope
     */
    public static TableScope allTables() {
        return ALL_TABLES;
    }

    /**
     * Creates a scope covering only the given tables.
     *
     * @param  tableIds                 the IDs of the tables
     * @return                          the scope
     * @throws IllegalArgumentException if no tables are given
     */
    public static TableScope tables(Collection<String> tableIds) {
        if (tableIds == null || tableIds.isEmpty()) {
            throw new IllegalArgumentException("A permission must be granted over at least one table. Use all tables "
                    + "to grant it over every table in the instance.");
        }
        tableIds.forEach(tableId -> {
            if (tableId == null || tableId.isBlank()) {
                throw new IllegalArgumentException("A table a permission is granted over must have an ID.");
            }
        });
        return new TableScope(Set.copyOf(tableIds));
    }

    /**
     * Checks whether a permission granted over this scope applies to the given table.
     *
     * @param  tableId the ID of the table
     * @return         true if it applies
     */
    public boolean includes(String tableId) {
        return tableIds.isEmpty() || tableIds.contains(tableId);
    }

    public boolean isAllTables() {
        return tableIds.isEmpty();
    }

    /**
     * Retrieves the tables this covers. Empty when it covers every table, which {@link #isAllTables} reports.
     *
     * @return the IDs of the tables
     */
    public Set<String> tableIds() {
        return tableIds;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TableScope)) {
            return false;
        }
        return tableIds.equals(((TableScope) object).tableIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableIds);
    }

    @Override
    public String toString() {
        return tableIds.isEmpty() ? "all tables" : tableIds.stream().sorted().toList().toString();
    }
}
