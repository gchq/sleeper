/*
 * Copyright 2022 Crown Copyright
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
import sleeper.configuration.properties.InstanceProperties;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_ARM_CPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_ARM_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_X86_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_ARM_GPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_X86_GPU;
public final class Requirements {

  private Requirements() {
  }

 /**
     * Retrieves architecture specific CPU and memory requirements. This returns
     * a triple containing the CPU requirement in the left element and memory
     * requirement in the middle and GPU requirement in the right element.
     *
     * @param architecture       CPU architecture
     * @param launchType         the container launch type
     * @param instanceProperties Sleeper instance properties
     * @return CPU, memory and GPU requirements as per the CPU architecture
     */
    private static Triple<Integer, Integer, Integer> getArchRequirements(String architecture,
            String launchType,
            InstanceProperties instanceProperties) {
        int cpu;
        int memoryLimitMiB;
        int gpu;
        if (architecture.startsWith("ARM")) {
            cpu = instanceProperties.getInt(COMPACTION_TASK_ARM_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_ARM_MEMORY);
            gpu = instanceProperties.getInt(COMPACTION_TASK_ARM_GPU);
        } else {
            cpu = instanceProperties.getInt(COMPACTION_TASK_X86_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_X86_MEMORY);
            gpu = instanceProperties.getInt(COMPACTION_TASK_X86_GPU);
        }
        return Triple.of(cpu, memoryLimitMiB, gpu);
    }
}
