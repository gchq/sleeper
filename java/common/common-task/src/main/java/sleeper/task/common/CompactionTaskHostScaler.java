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
package sleeper.task.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.CompactionTaskRequirements;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_FIXED_OVERHEAD;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PERCENTAGE_OVERHEAD;

/**
 * Autoscaler to scale EC2 instances for the desired number of compaction tasks. This makes decisions on how many
 * instances to start and stop based on the amount of work there is to do.
 */
public class CompactionTaskHostScaler {

    private final InstanceProperties instanceProperties;
    private final CheckAutoScalingGroup asgQuery;
    private final SetDesiredInstances asgUpdate;
    private final CheckInstanceType ec2Query;
    private final Map<String, InstanceType> instanceTypeCache = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(EC2Scaler.class);

    public CompactionTaskHostScaler(
            InstanceProperties instanceProperties,
            CheckAutoScalingGroup asgQuery, SetDesiredInstances asgUpdate, CheckInstanceType ec2Query) {
        this.instanceProperties = instanceProperties;
        this.asgQuery = asgQuery;
        this.asgUpdate = asgUpdate;
        this.ec2Query = ec2Query;
    }

    /**
     * Scales the ECS Auto Scaling Group to the right size. This looks at the number of total
     * compaction tasks that should be running and the number that can fit on one instance and adjusts the
     * desired size of the ASG.
     *
     * @param numberOfTasks total number of tasks to provide capacity for
     */
    public void scaleTo(int numberOfTasks) {
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        // Only need scaler for EC2
        if (!launchType.equalsIgnoreCase("EC2")) {
            return;
        }
        int containersPerInstance = lookupInstanceType()
                .map(this::computeContainersPerInstance)
                .orElse(1);
        String asScalingGroup = instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP);
        int maxInstances = asgQuery.getAutoScalingGroupMaxSize(asScalingGroup);

        int instancesDesired = (int) (Math.ceil(numberOfTasks / (double) containersPerInstance));
        int newClusterSize = Math.min(instancesDesired, maxInstances);
        LOGGER.info("Total containers wanted (including existing ones) {}, containers per instance {}, " +
                "so total instances wanted {}, limited to {} by ASG maximum size limit", numberOfTasks, containersPerInstance,
                instancesDesired, newClusterSize);

        asgUpdate.setClusterDesiredSize(asScalingGroup, newClusterSize);
    }

    private Optional<InstanceType> lookupInstanceType() {
        String ec2InstanceType = instanceProperties.get(COMPACTION_EC2_TYPE).toLowerCase(Locale.ROOT);
        try {
            return Optional.of(instanceTypeCache.computeIfAbsent(ec2InstanceType,
                    type -> ec2Query.getInstanceTypeInfo(type)));
        } catch (RuntimeException e) {
            LOGGER.error("Couldn't lookup EC2 type information for type " + ec2InstanceType, e);
            return Optional.empty();
        }
    }

    private int computeContainersPerInstance(InstanceType instanceType) {
        // ECS CPU reservation is done on scale of 1024 units = 100% of vCPU
        int cpuAvailable = instanceType.defaultVCpus() * 1024;
        long memoryMiB = getAvailableMemoryMiBWithoutOverhead(instanceType);
        LOGGER.debug("Computed availability per instance: {} CPU, {} memory MiB", cpuAvailable, memoryMiB);

        CompactionTaskRequirements requirements = getTaskRequirements();
        LOGGER.debug("Task requirements: {} CPU, {} memory MiB", requirements.getCpu(), requirements.getMemoryLimitMiB());

        int taskPerInstanceCpu = cpuAvailable / requirements.getCpu();
        int taskPerInstanceMemory = (int) (memoryMiB / requirements.getMemoryLimitMiB());
        int tasksPerInstance = Math.min(taskPerInstanceCpu, taskPerInstanceMemory);
        LOGGER.debug("Tasks per instance: {}", tasksPerInstance);
        if (tasksPerInstance < 1) {
            throw new IllegalArgumentException("" +
                    "Instance type does not fit a single compaction task with the configured requirements. " +
                    "CPU required " + requirements.getCpu() + ", found " + cpuAvailable + ". " +
                    "Memory MiB required " + requirements.getMemoryLimitMiB() + ", found " + memoryMiB + ".");
        }
        return tasksPerInstance;
    }

    private long getAvailableMemoryMiBWithoutOverhead(InstanceType instanceType) {
        // ECS can't use 100% of the memory on an EC2 for containers, and we also don't want to use the maximum
        // available capacity on an instance to avoid overloading them. Therefore, we reduce the available memory
        // advertised by an EC2 instance to accommodate this. This ensures we will create enough instances to hold
        // the desired number of containers. ECS will then be able to avoid allocating too many containers on to a
        // single instance.
        return instanceType.memoryMiB() - getMemoryOverheadMiB(instanceType);
    }

    private long getMemoryOverheadMiB(InstanceType instanceType) {
        Long fixedOverhead = instanceProperties.getLongOrNull(COMPACTION_TASK_FIXED_OVERHEAD);
        if (fixedOverhead != null) {
            return fixedOverhead;
        } else {
            double overheadPercent = instanceProperties.getLong(COMPACTION_TASK_PERCENTAGE_OVERHEAD) / 100.0;
            return (long) (instanceType.memoryMiB() * overheadPercent);
        }
    }

    private CompactionTaskRequirements getTaskRequirements() {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        return CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
    }

    public interface CheckAutoScalingGroup {
        int getAutoScalingGroupMaxSize(String autoScalingGroup);
    }

    public interface SetDesiredInstances {
        void setClusterDesiredSize(String autoScalingGroup, int desiredSize);
    }

    public interface CheckInstanceType {
        InstanceType getInstanceTypeInfo(String instanceType);
    }

    public record InstanceType(int defaultVCpus, long memoryMiB) {
    }

}
