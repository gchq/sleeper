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

package sleeper.core.statestore.exception;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStoreException;

/**
 * An exception for when a file could not be deleted because it still has references.
 */
public class FileHasReferencesException extends StateStoreException {
    public FileHasReferencesException(AllReferencesToAFile fileReferences) {
        this(fileReferences.getFilename(), fileReferences.getReferenceCount());
    }

    public FileHasReferencesException(String filename, int totalReferenceCount) {
        this(filename, totalReferenceCount, null);
    }

    public FileHasReferencesException(String filename, int totalReferenceCount, Exception cause) {
        super("Cannot remove file as it still has references, filename " + filename + ", " + totalReferenceCount + " references", cause);
    }
}
