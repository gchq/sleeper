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

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.SetDesiredCapacityRequest;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesResult;
import com.amazonaws.services.ec2.model.InstanceTypeInfo;
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
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;

/**
 * ECS EC2 auto scaler. This makes decisions on how many instances to start and stop based on the
 * amount of work there is to do.
 */
public class EC2Scaler {

    private final InstanceProperties instanceProperties;
    private final CheckAutoScalingGroup asgQuery;
    private final SetDesiredInstances asgUpdate;
    private final CheckEc2InstanceType ec2Query;
    private final Map<String, Ec2InstanceType> instanceTypeCache = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(EC2Scaler.class);

    public static EC2Scaler create(InstanceProperties instanceProperties, AmazonAutoScaling asClient, AmazonEC2 ec2Client) {
        return new EC2Scaler(instanceProperties,
                autoScalingGroup -> getAutoScalingGroupMaxSize(autoScalingGroup, asClient),
                (autoScalingGroup, desiredSize) -> setClusterDesiredSize(asClient, autoScalingGroup, desiredSize),
                instanceType -> getEc2InstanceType(ec2Client, instanceType));
    }

    public EC2Scaler(
            InstanceProperties instanceProperties,
            CheckAutoScalingGroup asgQuery, SetDesiredInstances asgUpdate, CheckEc2InstanceType ec2Query) {
        this.instanceProperties = instanceProperties;
        this.asgQuery = asgQuery;
        this.asgUpdate = asgUpdate;
        this.ec2Query = ec2Query;
    }

    /**
     * Scales the ECS Auto Scaling Group to the right size. This looks at the number of total
     * containers that should be running and the number that can fit on one instance and adjusts the
     * desired size of the ASG.
     *
     * @param numberContainers total number of containers to be run at the moment
     */
    public void scaleTo(int numberContainers) {
        int containersPerInstance = lookupInstanceType()
                .map(this::computeContainersPerInstance)
                .orElse(1);
        String asScalingGroup = instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP);
        int maxInstances = asgQuery.getAutoScalingGroupMaxSize(asScalingGroup);

        int instancesDesired = (int) (Math.ceil(numberContainers / (double) containersPerInstance));
        int newClusterSize = Math.min(instancesDesired, maxInstances);
        LOGGER.info("Total containers wanted (including existing ones) {}, containers per instance {}, " +
                "so total instances wanted {}, limited to {} by ASG maximum size limit", numberContainers, containersPerInstance,
                instancesDesired, newClusterSize);

        asgUpdate.setClusterDesiredSize(asScalingGroup, newClusterSize);
    }

    private Optional<Ec2InstanceType> lookupInstanceType() {
        String ec2InstanceType = instanceProperties.get(COMPACTION_EC2_TYPE).toLowerCase(Locale.ROOT);
        try {
            return Optional.of(instanceTypeCache.computeIfAbsent(ec2InstanceType,
                    type -> ec2Query.getEc2InstanceTypeInfo(type)));
        } catch (RuntimeException e) {
            LOGGER.error("Couldn't lookup EC2 type information for type " + ec2InstanceType, e);
            return Optional.empty();
        }
    }

    private int computeContainersPerInstance(Ec2InstanceType instanceType) {
        // ECS CPU reservation is done on scale of 1024 units = 100% of vCPU
        int cpuAvailable = instanceType.defaultVCpus() * 1024;
        // ECS can't use 100% of the memory on an EC2 for containers, and we also don't want to use the maximum
        // available capacity on an instance to avoid overloading them. Therefore, we reduce the available memory
        // advertised by an EC2 instance to accommodate this. This ensures we will create enough instances to hold
        // the desired number of containers. ECS will then be able to avoid allocating too many containers on to a
        // single instance.
        long memoryMiB = (long) (instanceType.memoryMiB() * 0.9);
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
                    "CPU required " + requirements.getCpu() + " of " + cpuAvailable + "." +
                    "Memory MiB required " + requirements.getMemoryLimitMiB() + " of " + memoryMiB + ".");
        }
        return tasksPerInstance;
    }

    private CompactionTaskRequirements getTaskRequirements() {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        return CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
    }

    /**
     * Find the details of a given EC2 auto scaling group.
     *
     * @param  groupName the name of the auto scaling group
     * @param  client    the client object
     * @return           group data
     */
    private static int getAutoScalingGroupMaxSize(String groupName, AmazonAutoScaling client) {
        DescribeAutoScalingGroupsRequest req = new DescribeAutoScalingGroupsRequest()
                .withAutoScalingGroupNames(groupName)
                .withMaxRecords(1);
        DescribeAutoScalingGroupsResult result = client.describeAutoScalingGroups(req);
        if (result.getAutoScalingGroups().size() != 1) {
            throw new IllegalStateException("instead of 1, received " + result.getAutoScalingGroups().size()
                    + " records for describe_auto_scaling_groups on group name " + groupName);
        }
        AutoScalingGroup group = result.getAutoScalingGroups().get(0);
        LOGGER.debug("Auto scaling group instance count: minimum {}, desired size {}, maximum size {}",
                group.getMinSize(), group.getDesiredCapacity(), group.getMaxSize());
        return group.getMaxSize();
    }

    private static Ec2InstanceType getEc2InstanceType(AmazonEC2 ec2Client, String instanceType) {
        DescribeInstanceTypesRequest request = new DescribeInstanceTypesRequest().withInstanceTypes(instanceType);
        DescribeInstanceTypesResult result = ec2Client.describeInstanceTypes(request);
        if (result.getInstanceTypes().size() != 1) {
            throw new IllegalStateException("got more than 1 result for DescribeInstanceTypes for type " + instanceType);
        }
        InstanceTypeInfo typeInfo = result.getInstanceTypes().get(0);
        LOGGER.info("EC2 instance type info: {}", typeInfo);
        return new Ec2InstanceType(
                typeInfo.getVCpuInfo().getDefaultVCpus(),
                typeInfo.getMemoryInfo().getSizeInMiB());
    }

    /**
     * Sets the desired size on the auto scaling group.
     *
     * @param newClusterSize new desired size to set
     */
    private static void setClusterDesiredSize(AmazonAutoScaling asClient, String asGroupName, int newClusterSize) {
        LOGGER.info("Setting auto scaling group {} desired size to {}", asGroupName, newClusterSize);
        SetDesiredCapacityRequest req = new SetDesiredCapacityRequest()
                .withAutoScalingGroupName(asGroupName)
                .withDesiredCapacity(newClusterSize);
        asClient.setDesiredCapacity(req);
    }

    public interface CheckAutoScalingGroup {
        int getAutoScalingGroupMaxSize(String autoScalingGroup);
    }

    public interface SetDesiredInstances {
        void setClusterDesiredSize(String autoScalingGroup, int desiredSize);
    }

    public interface CheckEc2InstanceType {
        Ec2InstanceType getEc2InstanceTypeInfo(String instanceType);
    }

    public record Ec2InstanceType(int defaultVCpus, long memoryMiB) {
    }
}
