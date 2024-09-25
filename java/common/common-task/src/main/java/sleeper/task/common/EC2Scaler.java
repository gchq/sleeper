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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.configuration.CompactionTaskRequirements;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;

/**
 * ECS EC2 auto scaler. This makes decisions on how many instances to start and stop based on the
 * amount of work there is to do.
 */
public class EC2Scaler {
    private final AmazonAutoScaling asClient;
    private final EcsClient ecsClient;
    /**
     * The name of the EC2 Auto Scaling group instances belong to.
     */
    private final String asGroupName;
    /**
     * The name of the ECS cluster the scaling group belongs to.
     */
    private final String ecsClusterName;
    /**
     * The number of containers each EC2 instance can host. -1 means we haven't found out yet.
     */
    private int cachedInstanceContainers = -1;
    /**
     * The CPU reservation for tasks.
     */
    private final int cpuReservation;
    /**
     * The memory reservation for tasks.
     */
    private final int memoryReservation;

    private static final Logger LOGGER = LoggerFactory.getLogger(EC2Scaler.class);

    public static EC2Scaler create(InstanceProperties instanceProperties, AmazonAutoScaling asClient, EcsClient ecsClient) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        // Bit hacky: EC2s don't give 100% of their memory for container use (OS
        // headroom, system tasks, etc.) so we have to make sure to reduce
        // the EC2 memory requirement by 5%. If we don't we end up asking for
        // 16GiB of RAM on a 16GiB box for example and container allocation will fail.
        int memoryLimitMiB = (int) (requirements.getMemoryLimitMiB() * 0.95);

        return new EC2Scaler(asClient, ecsClient, instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP),
                instanceProperties.get(COMPACTION_CLUSTER), requirements.getCpu(), memoryLimitMiB);
    }

    public EC2Scaler(AmazonAutoScaling asClient, EcsClient ecsClient, String asGroupName,
            String ecsClusterName, int cpuReservation, int memoryReservation) {
        this.asClient = asClient;
        this.ecsClient = ecsClient;
        this.asGroupName = asGroupName;
        this.ecsClusterName = ecsClusterName;
        this.cpuReservation = cpuReservation;
        this.memoryReservation = memoryReservation;
        LOGGER.debug("Scaler constraints: CPU reservation {} Memory reservation {}",
                this.cpuReservation, this.memoryReservation);
    }

    /**
     * Find out how many containers of a specific CPU and RAM requirement can fit into the cluster
     * at the moment.
     *
     * @param  instanceDetails cluster EC2 details
     * @return                 the number of containers that can fit
     */
    public int calculateAvailableClusterContainerCapacity(Map<String, EC2InstanceDetails> instanceDetails) {
        int total = 0;
        for (EC2InstanceDetails d : instanceDetails.values()) {
            total += Math.min(d.availableCPU / this.cpuReservation,
                    d.availableRAM / this.memoryReservation);
        }
        return total;
    }

    /**
     * Find the details of a given EC2 auto scaling group.
     *
     * @param  groupName the name of the auto scaling group
     * @param  client    the client object
     * @return           group data
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
        return result.getAutoScalingGroups().get(0);
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
        checkContainersPerInstance(null);

        // If we don't yet know how many can fit, then we assume only 1 will fit
        int containersPerInstance = (containerPerInstanceKnown()) ? this.cachedInstanceContainers : 1;

        // Retrieve the details of the scaling group
        AutoScalingGroup asg = getAutoScalingGroupInfo(asGroupName, asClient);
        LOGGER.debug("Auto scaling group current minimum {}, desired size {}, maximum size {}, containers per instance {}",
                asg.getMinSize(), asg.getDesiredCapacity(), asg.getMaxSize(), containersPerInstance);

        int instancesDesired = (int) (Math.ceil(numberContainers / (double) containersPerInstance));
        int newClusterSize = Math.min(instancesDesired, asg.getMaxSize());
        LOGGER.info("Total containers wanted (including existing ones) {}, containers per instance {}, " +
                "so total instances wanted {}, limited to {} by ASG maximum", numberContainers, containersPerInstance,
                instancesDesired, newClusterSize);

        // Set the new desired size on the cluster
        setClusterDesiredSize(newClusterSize);
    }

    /**
     * Sets the number of containers that can run on each instance. This method queries the cluster
     * to retrieve details of the machines in it if needed. This method makes the assumption that
     * the machines in the cluster are all identical. If details are passed in then they are used
     * otherwise a request is made to the ECS API.
     *
     * @param passedDetails optional details of cluster container instances, maybe null
     */
    private void checkContainersPerInstance(Map<String, EC2InstanceDetails> passedDetails) {
        if (containerPerInstanceKnown()) {
            return;
        }

        // If details were passed in, use them, otherwise find them ourselves
        Map<String, EC2InstanceDetails> details;
        if (passedDetails == null) {
            // fetch details from ECS cluster
            details = EC2InstanceDetails.fetchInstanceDetails(this.ecsClusterName, ecsClient);
        } else {
            details = passedDetails;
        }

        // Get the first one, we assume the containers are homogenous
        Optional<EC2InstanceDetails> det = details.values().stream().findFirst();
        det.ifPresent(d -> this.cachedInstanceContainers = Math.min(d.totalCPU / this.cpuReservation,
                d.totalRAM / this.memoryReservation));
    }

    /**
     * Whether we know how many containers can fit into an instance.
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
        LOGGER.info("Setting auto scaling group {} desired size to {}", this.asGroupName, newClusterSize);
        SetDesiredCapacityRequest req = new SetDesiredCapacityRequest()
                .withAutoScalingGroupName(asGroupName)
                .withDesiredCapacity(newClusterSize);
        asClient.setDesiredCapacity(req);
    }
}
