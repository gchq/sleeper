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

package sleeper.systemtest.drivers.util;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class WaitForTasksDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForTasksDriver.class);
    private final SleeperInstanceContext instance;
    private final AmazonElasticMapReduce emrClient;

    public WaitForTasksDriver(SleeperInstanceContext instance, AmazonElasticMapReduce emrClient) {
        this.instance = instance;
        this.emrClient = emrClient;
    }

    public static WaitForTasksDriver forEmr(SleeperInstanceContext instance, AmazonElasticMapReduce emrClient) {
        return new WaitForTasksDriver(instance, emrClient);
    }

    public void waitForTasksToStart(PollWithRetries pollUntilTasksStart) throws InterruptedException {
        String clusterPrefix = "sleeper-" + instance.getInstanceProperties().get(ID);
        pollUntilTasksStart.pollUntil("tasks have started", () -> {
            LOGGER.info("Checking for tasks that start with {}", clusterPrefix);
            return listRunningClusters(emrClient).getClusters().stream()
                    .anyMatch(cluster -> cluster.getName().startsWith(clusterPrefix));
        });
    }

    private static ListClustersResult listRunningClusters(AmazonElasticMapReduce emrClient) {
        return emrClient.listClusters(new ListClustersRequest()
                .withClusterStates(ClusterState.RUNNING));
    }
}
