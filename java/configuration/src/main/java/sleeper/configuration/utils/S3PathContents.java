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
package sleeper.configuration.utils;

import java.util.List;

/**
 * Results of expanding a path in S3.
 *
 * @param location the location in S3
 * @param files    the files found
 */
public record S3PathContents(S3Path location, List<S3FileDetails> files) {

    /**
     * Throws an exception if no files were found.
     *
     * @throws S3FileNotFoundException if no files were found
     */
    public void throwIfEmpty() throws S3FileNotFoundException {
        if (files.isEmpty()) {
            throw new S3FileNotFoundException(location.bucket(), location.pathInBucket());
        }
    }

    /**
     * Checks if no files were found.
     *
     * @return true if no files were found
     */
    public boolean isEmpty() {
        return files.isEmpty();
    }

    /**
     * Returns the path that was requested for expansion.
     *
     * @return the path
     */
    public String requestedPath() {
        return location.requestedPath();
    }
}
