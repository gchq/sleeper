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
 * A runtime exception to wrap failures in methods on a Sleeper state store.
 */
public class UncheckedStateStoreException extends RuntimeException {

    private final StateStoreException stateStoreException;

    public UncheckedStateStoreException(StateStoreException stateStoreException) {
        super(stateStoreException);
        this.stateStoreException = stateStoreException;
    }

    public StateStoreException getStateStoreException() {
        return stateStoreException;
    }
}