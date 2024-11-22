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
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup;
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsResponse;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;

import sleeper.configuration.CompactionTaskRequirements;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Locale;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;

/**
 * ECS EC2 auto scaler. This makes decisions on how many instances to start and stop based on the
 * amount of work there is to do.
 */
public class EC2Scaler {

    private final AutoScalingClient asClient;
    private final Ec2Client ec2Client;
    /**
     * The name of the EC2 Auto Scaling group instances belong to.
     */
    private final String asGroupName;
    /**
     * The number of containers each EC2 instance can host. -1 means we haven't found out yet.
     */
    private int cachedContainersPerInstance = -1;
    /**
     * The EC2 instance type being used.
     */
    private final String ec2InstanceType;
    /**
     * The CPU reservation for tasks.
     */
    private final int cpuReservation;
    /**
     * The memory reservation for tasks.
     */
    private final long memoryReservation;

    private static final Logger LOGGER = LoggerFactory.getLogger(EC2Scaler.class);

    public static EC2Scaler create(InstanceProperties instanceProperties, AutoScalingClient asClient, Ec2Client ec2Client) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        String asScalingGroup = instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP);
        String ec2InstanceType = instanceProperties.get(COMPACTION_EC2_TYPE).toLowerCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        return new EC2Scaler(asClient, ec2Client, asScalingGroup, ec2InstanceType, requirements.getCpu(), requirements.getMemoryLimitMiB());
    }

    public EC2Scaler(AutoScalingClient asClient, Ec2Client ec2Client, String asGroupName, String ec2InstanceType, int cpuReservation, int memoryReservation) {
        this.asClient = asClient;
        this.ec2Client = ec2Client;
        this.asGroupName = asGroupName;
        this.ec2InstanceType = ec2InstanceType;
        this.cpuReservation = cpuReservation;
        this.memoryReservation = memoryReservation;
        LOGGER.debug("Scaler constraints: CPU reservation {} Memory reservation {}",
                this.cpuReservation, this.memoryReservation);
    }

    /**
     * Find the details of a given EC2 auto scaling group.
     *
     * @param  groupName the name of the auto scaling group
     * @param  client    the client object
     * @return           group data
     */
    public static AutoScalingGroup getAutoScalingGroupInfo(String groupName, AutoScalingClient client) {
        DescribeAutoScalingGroupsResponse result = client.describeAutoScalingGroups(req -> req.autoScalingGroupNames(groupName).maxRecords(1));
        if (result.autoScalingGroups().size() != 1) {
            throw new IllegalStateException("instead of 1, received " + result.autoScalingGroups().size()
                    + " records for describe_auto_scaling_groups on group name " + groupName);
        }
        return result.autoScalingGroups().get(0);
    }

    /**
     * Scales the ECS Auto Scaling Group to the right size. This looks at the number of total
     * containers that should be running and the number that can fit on one instance and adjusts the
     * desired size of the ASG.
     *
     * @param numberContainers total number of containers to be run at the moment
     */
    public void scaleTo(int numberContainers) {
        scaleTo(asGroupName, numberContainers);
    }

    public void scaleTo(String asGroupName, int numberContainers) {
        // If we have any information set the number of containers per instance
        checkContainersPerInstance();

        int containersPerInstance = (containerPerInstanceKnown()) ? this.cachedContainersPerInstance : 1;

        // Retrieve the details of the scaling group
        AutoScalingGroup asg = getAutoScalingGroupInfo(asGroupName, asClient);
        LOGGER.debug("Auto scaling group instance count: minimum {}, desired size {}, maximum size {}, containers per instance {}",
                asg.minSize(), asg.desiredCapacity(), asg.maxSize(), containersPerInstance);

        int instancesDesired = (int) (Math.ceil(numberContainers / (double) containersPerInstance));
        int newClusterSize = Math.min(instancesDesired, asg.maxSize());
        LOGGER.info("Total containers wanted (including existing ones) {}, containers per instance {}, " +
                "so total instances wanted {}, limited to {} by ASG maximum size limit", numberContainers, containersPerInstance,
                instancesDesired, newClusterSize);

        // Set the new desired size on the cluster
        setClusterDesiredSize(newClusterSize);
    }

    /**
     * If the containers per EC2 instance has not been set, then make a request to AWS via EC2 service
     * describeInstanceTypes to find the size of the EC2 instance used for scaling. The amount of CPU and memory is then
     * used to work out how many containers per instance can fit.
     *
     * @throws IllegalStateException if more than one result is returned from AWS for describeInstanceTypes
     * @throws IllegalStateException if no containers at all can fit on the EC2 instance type set
     */
    private void checkContainersPerInstance() {
        if (containerPerInstanceKnown()) {
            return;
        }

        try {
            // Lookup instance type against AWS EC2
            DescribeInstanceTypesResponse result = ec2Client.describeInstanceTypes(req -> req.instanceTypesWithStrings(ec2InstanceType));
            if (result.instanceTypes().size() != 1) {
                throw new IllegalStateException("got more than 1 result for DescribeInstanceTypes for type " + ec2InstanceType);
            }
            InstanceTypeInfo typeInfo = result.instanceTypes().get(0);
            // ECS CPU reservation is done on scale of 1024 units = 100% of vCPU
            int vCPUCount = typeInfo.vCpuInfo().defaultVCpus() * 1024;
            // ECS can't use 100% of the memory on an EC2 for containers, and we also don't want to use the maximum
            // available capacity on an instance to avoid overloading them. Therefore, we reduce the available memory
            // advertised by an EC2 instance to accommodate this. This ensures we will create enough instances to hold
            // the desired number of containers. ECS will then be able to avoid allocating too many containers on to a
            // single instance.
            long memoryMiB = (long) (typeInfo.memoryInfo().sizeInMiB() * 0.9);
            this.cachedContainersPerInstance = Math.min(vCPUCount / this.cpuReservation,
                    (int) (memoryMiB / this.memoryReservation));
            if (cachedContainersPerInstance < 1) {
                throw new IllegalStateException(
                        "Can't fit any containers on to EC2 type " + this.ec2InstanceType + ". Container CPU reservation: " + this.cpuReservation + " memory: " + this.memoryReservation
                                + ". EC2 CPU: " + vCPUCount + " memory: " + memoryMiB);
            }

        } catch (SdkException e) {
            LOGGER.error("couldn't lookup EC2 type information for type " + this.ec2InstanceType, e);
        }
    }

    /**
     * Whether we know how many containers can fit into an instance.
     *
     * @return true if the value is known
     */
    private boolean containerPerInstanceKnown() {
        return this.cachedContainersPerInstance != -1;
    }

    /**
     * Sets the desired size on the auto scaling group.
     *
     * @param newClusterSize new desired size to set
     */
    public void setClusterDesiredSize(int newClusterSize) {
        LOGGER.info("Setting auto scaling group {} desired size to {}", this.asGroupName, newClusterSize);
        asClient.setDesiredCapacity(req -> req.autoScalingGroupName(asGroupName).desiredCapacity(newClusterSize));
    }
}
