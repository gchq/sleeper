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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InMemoryRevisionTrackedFileStore<T> implements RevisionTrackedS3FileType.Store<T> {

    private final Map<String, T> dataByPath = new HashMap<>();
    private String failOnLoad;

    @Override
    public T load(String path) throws StateStoreException {
        if (failOnLoad != null) {
            String message = failOnLoad;
            failOnLoad = null;
            throw new StateStoreException(message);
        }
        return dataByPath.get(path);
    }

    @Override
    public void write(T data, String path) throws IOException {
        dataByPath.put(path, data);
    }

    @Override
    public void delete(String path) throws StateStoreException {
        dataByPath.remove(path);
    }

    public void setFailureOnNextDataLoad(String message) {
        failOnLoad = message;
    }
}
