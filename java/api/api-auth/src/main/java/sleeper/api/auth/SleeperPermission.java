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

/**
 * Something a client of the Sleeper REST API may be allowed to do.
 */
public enum SleeperPermission {

    /**
     * Allows everything, including granting access to others.
     */
    ADMIN(false),

    READ_INSTANCE_CONFIGURATION(false),

    READ_TABLE_CONFIGURATION(true);

    private final boolean tableScoped;

    SleeperPermission(boolean tableScoped) {
        this.tableScoped = tableScoped;
    }

    /**
     * Checks whether this permission is granted and checked against particular tables, rather than the instance as a
     * whole.
     *
     * @return true if it applies to tables
     */
    public boolean isTableScoped() {
        return tableScoped;
    }

}
