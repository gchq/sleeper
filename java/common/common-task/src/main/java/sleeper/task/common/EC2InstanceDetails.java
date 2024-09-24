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

import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.ContainerInstance;
import software.amazon.awssdk.services.ecs.model.ContainerInstanceStatus;
import software.amazon.awssdk.services.ecs.model.Resource;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * Details about EC2 instances in an ECS cluster.
 */
public class EC2InstanceDetails {
    /**
     * EC2 Instance ID.
     */
    public final String instanceId;
    /**
     * The container instance ARN.
     */
    public final String instanceArn;
    /**
     * When was the instance registered with the cluster.
     */
    public final Instant registered;
    /**
     * Amount of RAM available for container use.
     */
    public final int availableCPU;
    /**
     * Amount of CPU available for container use.
     */
    public final int availableRAM;
    /**
     * Amount of CPU in total.
     */
    public final int totalCPU;
    /**
     * Amount of RAM in total.
     */
    public final int totalRAM;
    /**
     * Number of running tasks.
     */
    public final int numRunningTasks;
    /**
     * Number of pending tasks.
     */
    public final int numPendingTasks;

    /**
     * The number of ECS container instances to retrieve in one API call.
     */
    public static final int INSTANCE_PAGE_SIZE = 75;

    public EC2InstanceDetails(String instanceId, String instanceArn, Instant registered, int availableCPU,
            int availableRAM, int totalCPU,
            int totalRAM, int numRunningTasks, int numPendingTasks) {
        super();
        this.instanceId = instanceId;
        this.instanceArn = instanceArn;
        this.registered = registered;
        this.availableCPU = availableCPU;
        this.availableRAM = availableRAM;
        this.totalCPU = totalCPU;
        this.totalRAM = totalRAM;
        this.numRunningTasks = numRunningTasks;
        this.numPendingTasks = numPendingTasks;
    }

    /**
     * Find details of EC2 instances in an ECS cluster. Inspects the cluster to find the details of
     * all the instances.
     *
     * @param  ecsClusterName the cluster name
     * @param  ecsClient      the client connection
     * @return                map of instance IDs to details
     */
    public static Map<String, EC2InstanceDetails> fetchInstanceDetails(String ecsClusterName, EcsClient ecsClient) {
        return streamInstances(ecsClusterName, ecsClient)
                .collect(toMap(details -> details.instanceId, details -> details));
    }

    public static Stream<EC2InstanceDetails> streamInstances(String ecsClusterName, EcsClient ecsClient) {
        return ecsClient.listContainerInstancesPaginator(list -> list
                .cluster(ecsClusterName)
                .maxResults(INSTANCE_PAGE_SIZE)
                .status(ContainerInstanceStatus.ACTIVE))
                .stream()
                .flatMap(response -> ecsClient.describeContainerInstances(describe -> describe
                        .cluster(ecsClusterName)
                        .containerInstances(response.containerInstanceArns()))
                        .containerInstances().stream())
                .map(EC2InstanceDetails::from);
    }

    /**
     * Create an instance of this class for the given container.
     *
     * @param  container             the container
     * @return                       the new instance
     * @throws IllegalStateException if the container's CPU or memory does not match the expected type
     */
    private static EC2InstanceDetails from(ContainerInstance container) {
        // find the cpu and memory requirements
        List<Resource> totalResources = container.registeredResources();
        List<Resource> remainingResources = container.remainingResources();
        return new EC2InstanceDetails(
                container.ec2InstanceId(),
                container.containerInstanceArn(),
                container.registeredAt(),
                findResourceAmount("CPU", remainingResources),
                findResourceAmount("MEMORY", remainingResources),
                findResourceAmount("CPU", totalResources),
                findResourceAmount("MEMORY", totalResources),
                container.runningTasksCount(),
                container.pendingTasksCount());
    }

    /**
     * Find the amount of the given resource in the list of resources. The list is inspected for the
     * named resource and returned as an integer.
     *
     * @param  name                  the resource name to find
     * @param  resources             the list of resources
     * @return                       the amount, or 0 if not known
     * @throws IllegalStateException if the resource type is not INTEGER
     */
    private static int findResourceAmount(String name, List<Resource> resources) {
        for (Resource r : resources) {
            if (r.name().equals(name)) {
                if (!r.type().equals("INTEGER")) {
                    throw new java.lang.IllegalStateException(
                            "resource " + name + " has type " + r.type() + " instead of INTEGER");
                }
                return r.integerValue();
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return "InstanceDetails [instanceId=" + instanceId + ", instanceArn=" + instanceArn + ", registered=" + registered + ", availableCPU=" + availableCPU + ", availableRAM=" + availableRAM
                + ", totalCPU=" + totalCPU + ", totalRAM=" + totalRAM + ", numRunningTasks=" + numRunningTasks + ", numPendingTasks=" + numPendingTasks + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(availableCPU, availableRAM, instanceArn, instanceId, numPendingTasks, numRunningTasks, registered, totalCPU, totalRAM);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        EC2InstanceDetails other = (EC2InstanceDetails) obj;
        return availableCPU == other.availableCPU && availableRAM == other.availableRAM && Objects.equals(instanceArn, other.instanceArn) && Objects.equals(instanceId, other.instanceId)
                && numPendingTasks == other.numPendingTasks && numRunningTasks == other.numRunningTasks && Objects.equals(registered, other.registered) && totalCPU == other.totalCPU
                && totalRAM == other.totalRAM;
    }
}
