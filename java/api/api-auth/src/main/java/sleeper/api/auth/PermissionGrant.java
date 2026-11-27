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

import java.util.Objects;

/**
 * A permission granted to a principal, over the tables it applies to if it is table-scoped.
 */
public class PermissionGrant {

    private final SleeperPermission permission;
    private final TableScope tables;

    private PermissionGrant(SleeperPermission permission, TableScope tables) {
        this.permission = permission;
        this.tables = tables;
    }

    /**
     * Grants a permission that applies to the instance as a whole.
     *
     * @param  permission               the permission
     * @return                          the grant
     * @throws IllegalArgumentException if the permission applies to tables
     */
    public static PermissionGrant of(SleeperPermission permission) {
        Objects.requireNonNull(permission, "permission must not be null");
        if (permission.isTableScoped()) {
            throw new IllegalArgumentException(permission + " applies to tables, so it must be granted over the "
                    + "tables it applies to.");
        }
        return new PermissionGrant(permission, null);
    }

    /**
     * Grants a permission over the tables it applies to.
     *
     * @param  permission               the permission
     * @param  tables                   the tables it is granted over
     * @return                          the grant
     * @throws IllegalArgumentException if the permission applies to the instance as a whole
     */
    public static PermissionGrant of(SleeperPermission permission, TableScope tables) {
        Objects.requireNonNull(permission, "permission must not be null");
        Objects.requireNonNull(tables, "tables must not be null");
        if (!permission.isTableScoped()) {
            throw new IllegalArgumentException(permission + " applies to the instance as a whole, so it cannot be "
                    + "granted over particular tables.");
        }
        return new PermissionGrant(permission, tables);
    }

    public SleeperPermission getPermission() {
        return permission;
    }

    /**
     * Retrieves the tables this is granted over.
     *
     * @return                       the tables
     * @throws IllegalStateException if the permission applies to the instance as a whole
     */
    public TableScope getTables() {
        if (tables == null) {
            throw new IllegalStateException(permission + " applies to the instance as a whole, so it is not granted "
                    + "over any tables.");
        }
        return tables;
    }

    /**
     * Checks whether this grant allows a permission that applies to the instance as a whole.
     *
     * @param  required the permission needed
     * @return          true if this grant allows it
     */
    public boolean allows(SleeperPermission required) {
        return tables == null && permission == required;
    }

    /**
     * Checks whether this grant allows a permission against a particular table.
     *
     * @param  required the permission needed
     * @param  tableId  the ID of the table it is needed against
     * @return          true if this grant allows it
     */
    public boolean allows(SleeperPermission required, String tableId) {
        return permission == required && tables != null && tables.includes(tableId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PermissionGrant)) {
            return false;
        }
        PermissionGrant other = (PermissionGrant) object;
        return permission == other.permission && Objects.equals(tables, other.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(permission, tables);
    }

    @Override
    public String toString() {
        return tables == null ? permission.toString() : permission + " over " + tables;
    }
}
