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

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_CPU;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_MEMORY;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_X86_MEMORY;

/**
 * Resource requirements for a compaction task. This determines the resources to request from AWS Fargate or EC2.
 */
public final class CompactionTaskRequirements {

    private final int cpu;
    private final int memoryLimitMiB;

    public CompactionTaskRequirements(int cpu, int memoryLimitMiB) {
        this.cpu = cpu;
        this.memoryLimitMiB = memoryLimitMiB;
    }

    /**
     * Retrieves architecture specific CPU and memory requirements for a compaction task.
     *
     * @param  architecture       CPU architecture
     * @param  instanceProperties Sleeper instance properties
     * @return                    CPU and memory requirements as per the CPU architecture
     */
    public static CompactionTaskRequirements getArchRequirements(String architecture, InstanceProperties instanceProperties) {
        int cpu;
        int memoryLimitMiB;
        if (architecture.startsWith("ARM")) {
            cpu = instanceProperties.getInt(COMPACTION_TASK_ARM_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_ARM_MEMORY);
        } else {
            cpu = instanceProperties.getInt(COMPACTION_TASK_X86_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_X86_MEMORY);
        }
        return new CompactionTaskRequirements(cpu, memoryLimitMiB);
    }

    public int getCpu() {
        return cpu;
    }

    public int getMemoryLimitMiB() {
        return memoryLimitMiB;
    }
}
