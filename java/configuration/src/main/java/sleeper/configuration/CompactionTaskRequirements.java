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
package sleeper.configuration;

import org.apache.commons.lang3.tuple.Pair;

import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_CPU;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_MEMORY;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_X86_MEMORY;

public final class CompactionTaskRequirements {

    private CompactionTaskRequirements() {
    }

    /**
     * Retrieves architecture specific CPU and memory requirements. This returns a triple containing
     * the CPU requirement in the left element and memory requirement in the right element.
     *
     * @param  architecture       CPU architecture
     * @param  instanceProperties Sleeper instance properties
     * @return                    CPU and memory requirements as per the CPU architecture
     */
    public static Pair<Integer, Integer> getArchRequirements(String architecture, InstanceProperties instanceProperties) {
        int cpu;
        int memoryLimitMiB;
        if (architecture.startsWith("ARM")) {
            cpu = instanceProperties.getInt(COMPACTION_TASK_ARM_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_ARM_MEMORY);
        } else {
            cpu = instanceProperties.getInt(COMPACTION_TASK_X86_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_X86_MEMORY);
        }
        return Pair.of(cpu, memoryLimitMiB);
    }
}
