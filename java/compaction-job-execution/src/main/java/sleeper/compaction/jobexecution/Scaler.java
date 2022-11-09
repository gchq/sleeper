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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.SetDesiredCapacityRequest;
import com.amazonaws.services.autoscaling.model.TerminateInstanceInAutoScalingGroupRequest;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.ListTasksResult;
import com.amazonaws.services.ecs.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    /**
     * The length of time an EC2 can be idle before being a candidate for
     * termination.
     */
    private final Duration gracePeriod;

    private static final Logger LOGGER = LoggerFactory.getLogger(Scaler.class);

    public Scaler(AmazonAutoScaling asClient, AmazonECS ecsClient, String asGroupName,
            String ecsClusterName,
            int cpuReservation, int memoryReservation, Duration gracePeriod) {
        super();
        this.asClient = asClient;
        this.ecsClient = ecsClient;
        this.asGroupName = asGroupName;
        this.ecsClusterName = ecsClusterName;
        this.cpuReservation = cpuReservation;
        this.memoryReservation = memoryReservation;
        this.gracePeriod = gracePeriod;
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
     * @param details           map from EC2 instance ID to details about that
     *                          instance
     * @return result stating what action was taken
     */
    public ScaleOutResult possiblyScaleOut(int newContainerCount, Map<String, InstanceDetails> details) {
        // If we have any information set the number of containers per instance
        checkContainersPerInstance(details);

        // If we don't yet know how many can fit, then we assume only 1 will fit
        int containersPerInstance = (containerPerInstanceKnown()) ? this.cachedInstanceContainers : 1;

        int availableContainerCount = calculateAvailableClusterContainerCapacity(details);

        LOGGER.debug("newContainerCount {} containersPerInstance {} availableContainerCount {}", newContainerCount,
                containersPerInstance, availableContainerCount);
        // Do we need to scale out?
        if (newContainerCount <= availableContainerCount) {
            LOGGER.debug("No scale out required");
            return ScaleOutResult.NOT_REQUIRED;
        }
        LOGGER.info("Containers to launch {}, cluster capacity {}", newContainerCount, availableContainerCount);
        // Retrieve the details of the scaling group
        AutoScalingGroup asg = getAutoScalingGroupInfo(asGroupName, asClient);
        LOGGER.info(
                "Auto scaling group current minimum {}, desired size {}, maximum size {}, containers per instance {}",
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
        int newClusterSize = asg.getDesiredCapacity() + instancesAvailable;
        LOGGER.info("Current scaling group size is {}, want to launch {} instances, spare capacity is {}",
                asg.getDesiredCapacity(), instancesDesired, remainingHeadroom);
        LOGGER.info("Setting auto scaling group {} desired size to {}", this.asGroupName, newClusterSize);
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

    /**
     * Fetch a list of tasks from the ECS cluster that have the given task family
     * name and desired status.
     *
     * @param taskFamilyName the task family name to filter on
     * @param desiredStatus  task status should be RUNNING or STOPPED
     * @return the list of matching tasks
     */
    public List<String> getTasks(String taskFamilyName, String desiredStatus) {
        List<String> taskARNs = new ArrayList<>();
        ListTasksRequest request = new ListTasksRequest()
                .withCluster(ecsClusterName)
                .withFamily(taskFamilyName)
                .withDesiredStatus(desiredStatus);
        boolean more = true;
        while (more) {
            ListTasksResult result = ecsClient.listTasks(request);
            // More to come?
            more = result.getNextToken() != null;
            request = request.withNextToken(result.getNextToken());
            taskARNs.addAll(result.getTaskArns());
        }
        return taskARNs;
    }

    /**
     * Creates a list of EC2 container instance ARNs that are safe from termination.
     * Looking at the list of tasks, we enumerate the instances and find those
     * that haven't run a task recently and are not running any now.
     * 
     * @param clusterTasks the list of tasks this cluster is running/has run
     * @return set of EC2 instances to terminate
     */
    public Set<String> determineSafeInstanceContainerARNs(List<String> clusterTasks) {
        // Set of instance container ARNs that are either running tasks
        // or have recently stopped running a task
        Set<String> safeInstanceARNs = new HashSet<>();
        Instant now = Instant.now();
        // Step through in groups of 100
        for (int i = 0; i < clusterTasks.size(); i += 100) {
            List<String> subTaskList = clusterTasks.subList(i, Math.min(i + 100, clusterTasks.size()));
            DescribeTasksRequest request = new DescribeTasksRequest()
                    .withCluster(ecsClusterName)
                    .withTasks(subTaskList);
            DescribeTasksResult result = ecsClient.describeTasks(request);
            for (Task t : result.getTasks()) {

                String instanceArn = t.getContainerInstanceArn();
                if (instanceArn == null || instanceArn.trim().isEmpty()) {
                    continue; // Fargate tasks have no container instance
                }

                // Should we keep this task's EC2
                boolean shouldKeepEC2 = false;
                LOGGER.debug("Task {} running on {}, keep its EC2", t.getTaskArn(), instanceArn);
                // Keep running tasks
                if (t.getLastStatus().equals("RUNNING")) {
                    shouldKeepEC2 = true;
                } else if (t.getLastStatus().equals("STOPPED")) {
                    // When did the task stop?
                    Date stopped = t.getStoppedAt();
                    if (stopped == null) {
                        LOGGER.warn("Task has no stop time, but status is stopped!");
                        continue; // this shouldn't happen
                    }
                    Instant stopInstant = stopped.toInstant();
                    Duration stopDuration = Duration.between(stopInstant, now);
                    if (stopDuration.compareTo(gracePeriod) >= 0) {
                        LOGGER.debug("Task stopped longer than {} seconds, so don't keep its EC2",
                                gracePeriod.getSeconds());
                    } else {
                        LOGGER.debug("Task stopped recently ({} seconds), keep its EC2", stopDuration.getSeconds());
                        shouldKeepEC2 = true;
                    }
                }

                // Should we keep this EC2?
                if (shouldKeepEC2) {
                    safeInstanceARNs.add(instanceArn);
                }
            }
        }
        return safeInstanceARNs;
    }

    /**
     * Given a list of container instance ARNs that are safe, find EC2 IDs that are
     * not
     * on the safe list and haven't been registered with the cluster inside the
     * grace
     * period.
     * 
     * @param safeARNs the list of container instance ARNs that are safe from
     *                 termination
     * @param details  the cluster instance details
     * @return list of EC2 IDs to terminate
     */
    public List<String> findEC2IDsToTerminate(Set<String> safeARNs, Map<String, InstanceDetails> details) {
        List<String> terminationIDs = new ArrayList<>();
        Instant now = Instant.now();
        // If an instance's ARN is not in the safe list AND it's registration time is
        // older
        // than the grace period, it should be terminated
        for (Map.Entry<String, InstanceDetails> machine : details.entrySet()) {
            if (safeARNs.contains(machine.getValue().instanceArn)) {
                LOGGER.info("Instance ARN {} ID {} is in safe list, so keep it.",
                        machine.getValue().instanceArn, machine.getKey());
            } else {
                Duration uptime = Duration.between(machine.getValue().registered, now);
                if (uptime.compareTo(gracePeriod) >= 0) {
                    LOGGER.info("Instance ARN {} ID {} idle longer than grace period, so terminate it",
                            machine.getValue().instanceArn, machine.getKey());
                    terminationIDs.add(machine.getKey());
                } else {
                    LOGGER.info("Instance ARN {} ID {} still in grace period ({} seconds) so keep it",
                            machine.getValue().instanceArn, machine.getKey(), uptime.getSeconds());
                }
            }
        }
        return terminationIDs;
    }

    /**
     * Attempt to scale the cluster in.
     * 
     * @param taskFamilyName            the EC2 task family name
     * @param details                   the cluster details
     * @param containerInstanceARNsUsed list of instance container ARNs from
     *                                  recently
     *                                  launched tasks
     */
    public void possiblyScaleIn(String taskFamilyName, Map<String, InstanceDetails> details,
            Set<String> containerInstanceARNsUsed) {
        // List of container instance ARNs that are safe from termination
        Set<String> safeARNs = new HashSet<>(containerInstanceARNsUsed);

        // Get a list of all running and stopped tasks on the cluster
        List<String> clusterTasks = getTasks(taskFamilyName, "RUNNING");
        clusterTasks.addAll(getTasks(taskFamilyName, "STOPPED"));

        // Find the set of EC2 instance container ARNs that are running tasks
        // or recently stopped running tasks
        safeARNs.addAll(determineSafeInstanceContainerARNs(clusterTasks));

        // Take this list of safe ARNs and wash it against the list of all EC2s
        // to find ones to terminate
        List<String> terminationIDs = findEC2IDsToTerminate(safeARNs, details);

        // Terminate the IDs in the given list
        terminateInstances(terminationIDs);
    }

    /**
     * Terminate the EC2 IDs in the given list.
     * 
     * @param terminationIDs termination IDs
     */
    public void terminateInstances(List<String> terminationIDs) {
        for (String id : terminationIDs) {
            LOGGER.info("Attempting to terminate EC2 ID {}", id);
            try {
                TerminateInstanceInAutoScalingGroupRequest request = new TerminateInstanceInAutoScalingGroupRequest()
                        .withInstanceId(id)
                        .withShouldDecrementDesiredCapacity(true);
                asClient.terminateInstanceInAutoScalingGroup(request);
            } catch (AmazonClientException e) {
                LOGGER.error("Couldn't terminate EC2 ID " + id, e);
            }
        }
    }
}
