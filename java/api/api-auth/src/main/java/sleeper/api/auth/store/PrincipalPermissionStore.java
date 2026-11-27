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

/**
 * Holds what each principal is allowed to do.
 */
@FunctionalInterface
public interface PrincipalPermissionStore {

    /**
     * Retrieves what a principal is allowed to do.
     *
     * @param  principal the principal
     * @return           what they are allowed to do, or nothing if they have not been granted anything
     */
    PrincipalPermissions permissionsFor(PrincipalId principal);

}
