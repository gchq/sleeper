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
package sleeper.query.core.model;

/**
 * Stores the JSON of queries that are too large to be held in a single message on a queue.
 */
public interface QueryJsonStore {

    /**
     * Stores the JSON of a query.
     *
     * @param key  the key to store the JSON under
     * @param json the JSON
     */
    void store(String key, String json);

    /**
     * Retrieves the JSON of a query that was previously stored.
     *
     * @param  key the key the JSON was stored under
     * @return     the JSON
     */
    String load(String key);
}
