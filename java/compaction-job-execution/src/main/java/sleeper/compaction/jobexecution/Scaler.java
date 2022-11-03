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
import com.amazonaws.services.ecs.AmazonECS;

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
    /** The number of containers each EC2 instance can host. */
    private final int containersPerInstance;

    public Scaler(AmazonAutoScaling asClient, AmazonECS ecsClient, String asGroupName, String ecsClusterName) {
        this.asClient = asClient;
        this.ecsClient = ecsClient;
        this.asGroupName = asGroupName;
        this.ecsClusterName = ecsClusterName;
        this.containersPerInstance = calculateInstanceContainerCount();
    }

    /**
     * Calculates the number of containers that can fit into one EC2 instance.
     *
     * @return container count
     */
    private int calculateInstanceContainerCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    public int calculateAvailableClusterContainerCapacity() {

        return 0;
    }

    /**
     * Perform an EC2 auto scaling group scale-out if needed. First checks to see if
     * any more instances are needed to run the desired number of new containers.
     * If so, then the number of new instances is calculated from the remaining
     * headroom in the cluster and number of containers that can fit on the instance
     * type in the auto scaling group.
     *
     * @param newContainerCount the number of new containers desired
     * @param clusterSize       the current number of EC2 instances in the cluster
     * @return result stating what action was taken
     */
    public ScaleOutResult possiblyScaleOut(int newContainerCount, int clusterSize) {
        return null;
    }
}
