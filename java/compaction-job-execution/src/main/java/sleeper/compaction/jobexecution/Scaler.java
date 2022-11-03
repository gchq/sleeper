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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.SetDesiredCapacityRequest;
import com.amazonaws.services.autoscaling.model.SetDesiredCapacityResult;
import com.amazonaws.services.ecs.AmazonECS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * ECS EC2 auto scaler. This makes decisions on how many instances to start and
 * stop based on the amount of work there is to do.
 */
public class Scaler {
    /** AutoScaling client */
    private final AmazonAutoScaling asClient;
    /** ECS client */
    private final AmazonECS ecsClient;
    /** The name of the EC2 Auto Scaling group instances belong to. */
    private final String asGroupName;
    /** The name of the ECS cluster the scaling group belongs to. */
    private final String ecsClusterName;
    /**
     * The number of containers each EC2 instance can host. -1 means we haven't
     * found out yet.
     */
    private int cachedInstanceContainers = -1;
    /** The CPU reservation for tasks. */
    private final int cpuReservation;
    /** The memory reservation for tasks. */
    private final int memoryReservation;

    private static final Logger LOGGER = LoggerFactory.getLogger(Scaler.class);

    public Scaler(AmazonAutoScaling asClient, AmazonECS ecsClient, String asGroupName,
            String ecsClusterName,
            int cpuReservation, int memoryReservation) {
        super();
        this.asClient = asClient;
        this.ecsClient = ecsClient;
        this.asGroupName = asGroupName;
        this.ecsClusterName = ecsClusterName;
        this.cpuReservation = cpuReservation;
        this.memoryReservation = memoryReservation;
    }

    /**
     * Find out how many containers of a specific CPU and RAM requirement can fit
     * into the cluster at the moment.
     *
     * @param instanceDetails cluster EC2 details
     * @return the number of containers that can fit
     */
    public int calculateAvailableClusterContainerCapacity(Map<String, InstanceDetails> instanceDetails) {
        int total = 0;
        for (InstanceDetails d : instanceDetails.values()) {
            total += Math.min(d.availableCPU / this.cpuReservation, d.availableRAM / this.memoryReservation);
        }
        return total;
    }

    /**
     * Find the details of a given EC2 auto scaling group
     *
     * @param groupName the name of the auto scaling group
     * @param client    the client object
     * @return group data
     */
    public static AutoScalingGroup getAutoScalingGroupInfo(String groupName, AmazonAutoScaling client) {
        DescribeAutoScalingGroupsRequest req = new DescribeAutoScalingGroupsRequest()
                .withAutoScalingGroupNames(groupName)
                .withMaxRecords(1);
        DescribeAutoScalingGroupsResult result = client.describeAutoScalingGroups(req);
        if (result.getAutoScalingGroups().size() != 1) {
            throw new IllegalStateException("instead of 1, received " + result.getAutoScalingGroups().size()
                    + " records for describe_auto_scaling_groups on group name " + groupName);
        }
        AutoScalingGroup asg = result.getAutoScalingGroups().get(0);
        return asg;
    }

    /**
     * Perform an EC2 auto scaling group scale-out if needed. First checks to see if
     * any more instances are needed to run the desired number of new containers.
     * If so, then the number of new instances is calculated from the remaining
     * headroom in the cluster and number of containers that can fit on the instance
     * type in the auto scaling group.
     *
     * @param newContainerCount the number of new containers desired
     * @return result stating what action was taken
     */
    public ScaleOutResult possiblyScaleOut(int newContainerCount) {
        Map<String, InstanceDetails> details = InstanceDetails.fetchInstanceDetails(ecsClusterName, ecsClient);

        // If we have any information set the number of containers per instance
        checkContainersPerInstance(details);

        // If we don't yet know how many can fit, then we assume only 1 will fit
        int containersPerInstance = (containerPerInstanceKnown()) ? this.cachedInstanceContainers : 1;

        int availableContainerCount = calculateAvailableClusterContainerCapacity(details);
        // Do we need to scale out?
        if (newContainerCount <= availableContainerCount) {
            LOGGER.debug("No scale out required");
            return ScaleOutResult.NOT_REQUIRED;
        }
        LOGGER.info("Containers to launch %d, cluster capacity %d", newContainerCount, availableContainerCount);
        // Retrieve the details of the scaling group
        AutoScalingGroup asg = getAutoScalingGroupInfo(asGroupName, asClient);
        LOGGER.info(
                "Auto scaling group current minimum %d, desired size %d, maximum size %d, containers per instance %d",
                asg.getMinSize(), asg.getDesiredCapacity(), asg.getMaxSize(), containersPerInstance);

        // Are we able to provision any more instances?
        int remainingHeadroom = asg.getMaxSize() - asg.getDesiredCapacity();
        if (remainingHeadroom == 0) {
            LOGGER.info("Can't scale out anymore");
            return ScaleOutResult.NO_SPARE_HEADROOM;
        }

        // Are currently scaling?
        if (asg.getDesiredCapacity() != details.size()) {
            LOGGER.info("Scaling already in action, won't scale again");
            return ScaleOutResult.SCALING_IN_PROGRESS;
        }

        // How many instances should we start?
        int instancesDesired = (int) (Math.ceil(newContainerCount / (double) containersPerInstance));
        int instancesAvailable = Math.min(instancesDesired, remainingHeadroom);
        LOGGER.info("Want to launch %d instances, but only have capacity for %d", instancesDesired, instancesAvailable);
        int newClusterSize = asg.getDesiredCapacity() + instancesAvailable;

        // Set the new desired size on the cluster
        setClusterDesiredSize(newClusterSize);
        return ScaleOutResult.SCALING_INITIATED;
    }

    /**
     * Sets the number of containers that can run on each instance.
     * If there are any details given in the map, then the first machine is queried
     * to work out how many containers could fit on it.
     * This method makes the assumption that the machines in the cluster are all
     * identical.
     *
     * @param details details of machines in the ECS cluster
     */
    private void checkContainersPerInstance(Map<String, InstanceDetails> details) {
        if (containerPerInstanceKnown()) {
            return;
        }
        // get the first one, we assume the containers are homogenous
        Optional<InstanceDetails> det = details.values().stream().findFirst();
        det.ifPresent(d -> {
            this.cachedInstanceContainers = Math.min(d.totalCPU / this.cpuReservation,
                    d.totalRAM / this.memoryReservation);
        });
    }

    /**
     * Do we know how many containers can fit into an instance?
     *
     * @return true if the value is known
     */
    private boolean containerPerInstanceKnown() {
        return this.cachedInstanceContainers != -1;
    }

    /**
     * Sets the desired size on the auto scaling group.
     *
     * @param newClusterSize new desired size to set
     */
    public void setClusterDesiredSize(int newClusterSize) {
        SetDesiredCapacityRequest req = new SetDesiredCapacityRequest()
                .withAutoScalingGroupName(asGroupName)
                .withDesiredCapacity(newClusterSize);
        asClient.setDesiredCapacity(req);
    }
}
