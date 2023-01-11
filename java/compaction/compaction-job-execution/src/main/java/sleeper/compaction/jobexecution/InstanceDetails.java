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

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.ContainerInstance;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesRequest;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesResult;
import com.amazonaws.services.ecs.model.ListContainerInstancesRequest;
import com.amazonaws.services.ecs.model.ListContainerInstancesResult;
import com.amazonaws.services.ecs.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;

/**
 * Details about EC2 instances in an ECS cluster.
 */
public class InstanceDetails {
    /** EC2 Instance ID. */
    public final String instanceId;
    /** The container instance ARN. */
    public final String instanceArn;
    /** When was the instance registered with the cluster. */
    public final Instant registered;
    /** Amount of RAM available for container use. */
    public final int availableCPU;
    /** Amount of CPU available for container use. */
    public final int availableRAM;
    /** Amount of CPU in total. */
    public final int totalCPU;
    /** Amount of RAM in total. */
    public final int totalRAM;
    /** Number of running tasks. */
    public final int numRunningTasks;
    /** Number of pending tasks. */
    public final int numPendingTasks;

    /** The number of ECS container instances to retrieve in one API call. */
    public final static int INSTANCE_PAGE_SIZE = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceDetails.class);

    public InstanceDetails(String instanceId, String instanceArn, Instant registered, int availableCPU,
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
     * @param ecsClusterName the cluster name
     * @param ecsClient the client connection
     * @return map of instance IDs to details
     */
    public static Map<String, InstanceDetails> fetchInstanceDetails(String ecsClusterName, AmazonECS ecsClient) {
        Map<String, InstanceDetails> details = new HashMap<>();
        for (InstanceDetails d : iterateInstances(ecsClusterName, ecsClient)) {
            details.put(d.instanceId, d);
        }
        return details;
    }

    /**
     * Gets an {@link java.lang.Iterable} object that iterates over the instance details of machines
     * in a cluster.
     *
     * @param ecsClusterName ECS cluster name to inspect
     * @param ecsClient Amazon ECS client
     * @return iterable object for instances in a cluster
     */
    public static Iterable<InstanceDetails> iterateInstances(String ecsClusterName, AmazonECS ecsClient) {
        return new InstanceDetailsIterable(ecsClusterName, ecsClient);
    }

    /**
     * Class that iterates over EC2 machines in a cluster.
     */
    private static class InstanceDetailsIterable implements Iterable<InstanceDetails> {
        /** The ECS cluster name. */
        public final String ecsClusterName;
        /** Amazon client for HTTP requests to AWS. */
        public final AmazonECS ecsClient;

        public InstanceDetailsIterable(String ecsClusterName, AmazonECS ecsClient) {
            this.ecsClusterName = Objects.requireNonNull(ecsClusterName, "ecsClusterName");
            this.ecsClient = Objects.requireNonNull(ecsClient, "ecsClient");
        }

        @Override
        public Iterator<InstanceDetails> iterator() {
            return new InstanceDetailsIterator(INSTANCE_PAGE_SIZE);
        }

        private class InstanceDetailsIterator implements Iterator<InstanceDetails> {
            /** How many EC2 instances to get data for in one API call. */
            private final int pageSize;
            /** Request that gets modified as we fetch more pages. */
            private ListContainerInstancesRequest req;
            /** Has AWS indicated another page of results is waiting? */
            private boolean anotherPageWaiting = true;
            /** Queue to serve instances from. */
            private Queue<InstanceDetails> instanceQueue = new ArrayDeque<>();

            public InstanceDetailsIterator(int pageSize) {
                if (pageSize < 1) {
                    throw new IllegalArgumentException("pageSize must be > 0");
                }
                this.pageSize = pageSize;
                this.req = new ListContainerInstancesRequest()
                                .withCluster(ecsClusterName)
                                .withMaxResults(pageSize)
                                .withStatus("ACTIVE");
            }

            private boolean refillQueue() {
                if (anotherPageWaiting) {
                    LOGGER.debug("Retrieving upto {} instances for ECS cluster {}", pageSize, ecsClusterName);
                    ListContainerInstancesResult result = ecsClient.listContainerInstances(req);
                    // More to come?
                    anotherPageWaiting = result.getNextToken() != null;
                    req = req.withNextToken(result.getNextToken());
                    // check to see if there are any at all
                    if (result.getContainerInstanceArns().isEmpty()) {
                        anotherPageWaiting = false;
                        return false;
                    }
                    // now get a description of these instances
                    DescribeContainerInstancesRequest conReq = new DescribeContainerInstancesRequest()
                                    .withCluster(ecsClusterName)
                                    .withContainerInstances(result.getContainerInstanceArns());
                    DescribeContainerInstancesResult containersResult = ecsClient.describeContainerInstances(conReq);
                    LOGGER.debug("Received details on {} instances", containersResult.getContainerInstances().size());
                    for (ContainerInstance c : containersResult.getContainerInstances()) {
                        // find the cpu and memory requirements
                        List<Resource> totalResources = c.getRegisteredResources();
                        List<Resource> remainingResources = c.getRemainingResources();
                        instanceQueue.add(new InstanceDetails(
                                        c.getEc2InstanceId(),
                                        c.getContainerInstanceArn(),
                                        c.getRegisteredAt().toInstant(),
                                        findResourceAmount("CPU", remainingResources),
                                        findResourceAmount("MEMORY", remainingResources),
                                        findResourceAmount("CPU", totalResources),
                                        findResourceAmount("MEMORY", totalResources),
                                        c.getRunningTasksCount(),
                                        c.getPendingTasksCount()));
                    }
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public boolean hasNext() {
                if (instanceQueue.isEmpty()) {
                    // queue empty, attempt to get more items
                    return refillQueue();
                } else { // items in queue, so next item definitely exists
                    return true;
                }
            }

            @Override
            public InstanceDetails next() {
                if (hasNext()) {
                    return instanceQueue.remove();
                } else {
                    throw new NoSuchElementException("no more instances");
                }
            }
        }
    }

    /**
     * Find the amount of the given resource in the list of resources. The list is inspected for the
     * named resource and returned as an integer.
     *
     * @param name the resource name to find
     * @param resources the list of resources
     * @return the amount, or 0 if not known
     * @throws IllegalStateException if the resource type is not INTEGER
     */
    public static int findResourceAmount(String name, List<Resource> resources) {
        for (Resource r : resources) {
            if (r.getName().equals(name)) {
                if (!r.getType().equals("INTEGER")) {
                    throw new java.lang.IllegalStateException(
                                    "resource " + name + " has type " + r.getType() + " instead of INTEGER");
                }
                return r.getIntegerValue();
            }
        }
        return 0;
    }

    public static void main(String... args) {
        Map<String, InstanceDetails> m = InstanceDetails.fetchInstanceDetails(args[0],
                        AmazonECSClientBuilder.defaultClient());
        System.out.println(m);
    }
}
