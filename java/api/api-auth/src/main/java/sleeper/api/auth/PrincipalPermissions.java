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
 * Everything a principal is allowed to do.
 */
public class PrincipalPermissions {

    private static final PrincipalPermissions NONE = new PrincipalPermissions(Set.of());
    private static final PrincipalPermissions ADMIN = new PrincipalPermissions(
            Set.of(PermissionGrant.of(SleeperPermission.ADMIN)));

    private final Set<PermissionGrant> grants;

    private PrincipalPermissions(Set<PermissionGrant> grants) {
        this.grants = grants;
    }

    /**
     * Allows nothing. An unknown principal.
     *
     * @return the permissions
     */
    public static PrincipalPermissions none() {
        return NONE;
    }

    /**
     * Allows everything, including granting access to others.
     *
     * @return the permissions
     */
    public static PrincipalPermissions admin() {
        return ADMIN;
    }

    /**
     * Allows what the given grants allow.
     *
     * @param  grants the grants
     * @return        the permissions
     */
    public static PrincipalPermissions of(Collection<PermissionGrant> grants) {
        Objects.requireNonNull(grants, "grants must not be null");
        return grants.isEmpty() ? NONE : new PrincipalPermissions(Set.copyOf(grants));
    }

    /**
     * Checks whether a permission that applies to the instance as a whole is allowed.
     *
     * @param  required                 the permission needed
     * @return                          true if it is allowed
     * @throws IllegalArgumentException if the permission applies to tables
     */
    public boolean allows(SleeperPermission required) {
        Objects.requireNonNull(required, "required must not be null");
        if (required.isTableScoped()) {
            throw new IllegalArgumentException(required + " applies to tables, so the table it is needed against must be given.");
        }
        return isAdmin() || grants.stream().anyMatch(grant -> grant.allows(required));
    }

    /**
     * Checks whether a permission is allowed against a particular table.
     *
     * @param  required                 the permission needed
     * @param  tableId                  the ID of the table it is needed against
     * @return                          true if it is allowed
     * @throws IllegalArgumentException if the permission applies to the instance as a whole
     */
    public boolean allows(SleeperPermission required, String tableId) {
        Objects.requireNonNull(required, "required must not be null");
        if (!required.isTableScoped()) {
            throw new IllegalArgumentException(required + " applies to the instance as a whole, so it is not held against a table.");
        }
        return isAdmin() || grants.stream().anyMatch(grant -> grant.allows(required, tableId));
    }

    /**
     * Checks whether the principal is an administrator.
     *
     * @return true if the principal is an administrator
     */
    public boolean isAdmin() {
        return grants.stream().anyMatch(grant -> grant.allows(SleeperPermission.ADMIN));
    }

    /**
     * Checks whether nothing is allowed.
     *
     * @return true if the principal is allowed nothing
     */
    public boolean isEmpty() {
        return grants.isEmpty();
    }

    public Set<PermissionGrant> getGrants() {
        return grants;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PrincipalPermissions)) {
            return false;
        }
        return grants.equals(((PrincipalPermissions) object).grants);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grants);
    }

    @Override
    public String toString() {
        return grants.isEmpty() ? "nothing" : grants.toString();
    }
}
