/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.compaction.jobexecution;

import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.util.Locale;

/**
 * Different compaction methods for Sleeper which support different capabilities and must be
 * selected based on need.
 */
public enum CompactionMethod {
    /** Pure Java compaction implementation. */
    JAVA {
        @Override
        public boolean supportsIterators() {
            return true;
        }

        @Override
        public boolean supportsSplittingCompactions() {
            return true;
        }
    },
    /**
     * Rust accelerated compaction method. This uses a native library written in Rust to perform a
     * compaction.
     */
    RUST {
        @Override
        public boolean supportsIterators() {
            return false;
        }

        @Override
        public boolean supportsSplittingCompactions() {
            return false;
        }
    };

    public static final CompactionMethod DEFAULT = CompactionMethod.JAVA;

    /**
     * @return true if this compaction method can be used for splitting compactions
     */
    public abstract boolean supportsSplittingCompactions();

    /**
     * @return true if this compaction method supports tables with iterators present
     */
    public abstract boolean supportsIterators();

    /**
     * Retrieve the compaction method to use based on the job configuration and the table
     * properties. If a compaction method doesn't support all the features needed for this job, then
     * an alternative compaction method will be chosen.
     *
     * @param tableProperties configuration for the table being compacted
     * @param job the compaction details
     * @return appropriate compaction method
     */
    public static CompactionMethod determineCompactionMethod(TableProperties tableProperties,
            CompactionJob job) {
        // First find desired compaction method
        String configMethod = tableProperties.get(TableProperty.COMPACTION_METHOD)
                .toUpperCase(Locale.ROOT);

        // Convert to enum value and default to Java
        CompactionMethod desired;
        try {
            desired = CompactionMethod.valueOf(configMethod);
        } catch (IllegalArgumentException e) {
            desired = CompactionMethod.JAVA;
        }

        // Is this a splitting compaction? If yes, but method doesn't support it, then bail
        if (job.isSplittingJob() && !desired.supportsSplittingCompactions()) {
            return CompactionMethod.JAVA;
        }

        // Is an iterator specifed, if so can we support this?
        if (job.getIteratorClassName() != null && !desired.supportsIterators()) {
            return CompactionMethod.JAVA;
        }

        return desired;
    }
}
