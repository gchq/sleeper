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

package sleeper.core.properties;

/**
 * Reloads cached configuration properties when trigged if configured to do so.
 */
public interface PropertiesReloader {

    /**
     * Triggers reloading instance and/or table properties if needed.
     */
    void reloadIfNeeded();

    /**
     * Creates a properties reloader that will never reload any properties.
     *
     * @return the reloader
     */
    static PropertiesReloader neverReload() {
        return () -> {
        };
    }
}
