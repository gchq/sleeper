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
package sleeper.api.auth.store;

import sleeper.api.auth.PrincipalId;
import sleeper.api.auth.PrincipalPermissions;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds permissions in memory.
 */
public class InMemoryPrincipalPermissionStore implements PrincipalPermissionStore {

    private final Map<PrincipalId, PrincipalPermissions> permissionsByPrincipal = new HashMap<>();

    /**
     * Grants permissions to a principal, replacing anything previously granted to them.
     *
     * @param  principal   the principal
     * @param  permissions what they are allowed to do
     * @return             this store
     */
    public InMemoryPrincipalPermissionStore grant(PrincipalId principal, PrincipalPermissions permissions) {
        permissionsByPrincipal.put(principal, permissions);
        return this;
    }

    @Override
    public PrincipalPermissions permissionsFor(PrincipalId principal) {
        return permissionsByPrincipal.getOrDefault(principal, PrincipalPermissions.none());
    }

}
