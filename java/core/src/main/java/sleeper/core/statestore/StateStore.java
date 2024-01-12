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

/**
 * Stores information about the data files and their status (i.e. {@link FileReference}s,
 * and the {@link sleeper.core.partition.Partition}s).
 */
public interface StateStore extends FileInfoStore, PartitionStore {
    /**
     * Clears all file data and partition data from the state store.
     * <p>
     * Note that this does not delete any of the actual files, and after calling this
     * method the partition store must be initialised before the Sleeper table can be used again.
     */
    default void clearSleeperTable() {
        clearFileData();
        clearPartitionData();
    }
}
