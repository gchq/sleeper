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

package sleeper.core.properties.validation;

import org.apache.commons.lang3.EnumUtils;

/**
 * Determines how files are created and referenced during standard ingest.
 */
public enum IngestFileWritingStrategy {
    /**
     * Write a new file in each relevant leaf partition.
     */
    ONE_FILE_PER_LEAF,
    /**
     * Write one file, and add references to that file to all relevant leaf partitions.
     */
    ONE_REFERENCE_PER_LEAF;

    /**
     * Checks if the value is a valid ingest file writing strategy.
     *
     * @param  value the value
     * @return       true if it is valid
     */
    public static boolean isValid(String value) {
        return EnumUtils.isValidEnumIgnoreCase(IngestFileWritingStrategy.class, value);
    }
}
