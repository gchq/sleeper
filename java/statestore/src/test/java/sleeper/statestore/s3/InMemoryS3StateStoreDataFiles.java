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

package sleeper.statestore.s3;

import sleeper.core.statestore.StateStoreException;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-memory implementation of storing state store state in S3. Used in tests with {@link S3StateStoreDataFile}.
 *
 * @param <T> type of the state stored
 */
public class InMemoryS3StateStoreDataFiles<T> {

    private final Map<String, T> dataByPath = new HashMap<>();
    private String failOnLoad;
    private String failOnWrite;

    /**
     * Retrieves the data held at a given path.
     *
     * @param  path                the path
     * @return                     the data
     * @throws StateStoreException if the store was configured to fail to load data
     */
    public T load(String path) throws StateStoreException {
        if (failOnLoad != null) {
            String message = failOnLoad;
            failOnLoad = null;
            throw new StateStoreException(message);
        }
        return dataByPath.get(path);
    }

    /**
     * Writes data at a given path.
     *
     * @param  data                the data
     * @param  path                the path
     * @throws StateStoreException if the store was configured to fail to write data
     */
    public void write(T data, String path) throws StateStoreException {
        if (failOnWrite != null) {
            String message = failOnWrite;
            failOnWrite = null;
            throw new StateStoreException(message);
        }
        dataByPath.put(path, data);
    }

    /**
     * Deletes data at a given path.
     *
     * @param path the path
     */
    public void delete(String path) {
        dataByPath.remove(path);
    }

    public void setFailureOnNextDataLoad(String message) {
        failOnLoad = message;
    }

    public void setFailureOnNextDataWrite(String message) {
        failOnWrite = message;
    }

    static String buildPathFromRevisionId(S3RevisionId revisionId) {
        return "files/" + revisionId.getUuid();
    }
}
