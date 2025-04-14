/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.core.statestore.exception;

import sleeper.core.statestore.StateStoreException;

/**
 * An exception for when we could not replace a file with a new one because the new one is the same as the old one.
 */
public class NewReferenceSameAsOldReferenceException extends StateStoreException {
    public NewReferenceSameAsOldReferenceException(String filename) {
        super("New file has the same filename as a file being removed: " + filename);
    }
}
