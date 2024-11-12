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
package sleeper.core.statestore;

import java.time.Instant;

/**
 * Stores information about the data files in a Sleeper table. This includes a count of the number of references
 * to the file, and internal references which assign all the data in the file to non-overlapping partitions.
 */
public interface FileReferenceStore extends FileReferenceStoreQueries, FileReferenceStoreUpdates {

    /**
     * Performs extra setup steps that are needed before the file reference store can be used.
     *
     * @throws StateStoreException if initialisation fails
     */
    void initialise() throws StateStoreException;

    /**
     * Used to fix the time of file updates. Should only be called during tests.
     *
     * @param time the time that any future file updates will be considered to occur
     */
    void fixFileUpdateTime(Instant time);
}
