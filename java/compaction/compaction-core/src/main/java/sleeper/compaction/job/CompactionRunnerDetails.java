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
package sleeper.compaction.job;

public interface CompactionRunnerDetails {
    /**
     * Some compaction implementations may use hardware acceleration such as GPUs.
     *
     * @return true iff this compaction implementation uses any sort of hardware acceleration
     */
    default boolean isHardwareAccelerated() {
        return false;
    }

    /**
     * What language is this implemented in? If multiple languages are used, the primary
     * one used for performing the compaction computation should be returned.
     *
     * @return the principal implementation language for this compactor
     */
    default String implementationLanguage() {
        return "Java";
    }

    /**
     * States whether this compactor compact Sleeper tables that have iterators attached to them.
     *
     * @return true if iterators can be processed by this compactor
     */
    default boolean supportsIterators() {
        return false;
    }
}
