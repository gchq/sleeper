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
package sleeper.configuration.properties.validation;

import org.apache.commons.lang3.EnumUtils;

/**
 * Different compaction methods for Sleeper which support different capabilities and must be
 * selected based on need.
 */
public enum CompactionMethod {
    /** Pure Java compaction implementation. */
    JAVA,
    /** Uses a native library written in Rust to perform a compaction with Apache DataFusion. */
    DATAFUSION;

    /**
     * Checks if the value is a valid compaction method.
     *
     * @param  value the value
     * @return       true if it is valid
     */
    public static boolean isValid(String value) {
        return EnumUtils.isValidEnumIgnoreCase(CompactionMethod.class, value);
    }
}
