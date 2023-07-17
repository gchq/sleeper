/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.bulkimport;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class WaitForEMRClusters {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForEMRClusters.class);
    private static final long POLL_INTERVAL_MILLIS = 60000;
    private static final int MAX_POLLS = 30;
    private static final List<String> ACTIVE_STATES = List.of(
            ClusterState.STARTING.name(), ClusterState.BOOTSTRAPPING.name(),
            ClusterState.RUNNING.name(), ClusterState.WAITING.name(), ClusterState.TERMINATING.name());

    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    private final AmazonElasticMapReduce emrClient;
    private final String clusterPrefix;

    public WaitForEMRClusters(AmazonElasticMapReduce emrClient, InstanceProperties properties) {
        this.emrClient = emrClient;
        this.clusterPrefix = "sleeper-" + properties.get(ID) + "-system-test";
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("all EMR clusters finished", this::allClustersFinished);
    }

    public boolean allClustersFinished() {
        List<ClusterSummary> clusters = emrClient.listClusters(new ListClustersRequest())
                .getClusters().stream()
                .filter(cluster -> cluster.getName().startsWith(clusterPrefix))
                .collect(Collectors.toList());
        long active = countClustersWithState(clusters, ACTIVE_STATES);
        LOGGER.info("Found {} clusters, {} active, states: {}",
                clusters.size(), active, countClustersByState(clusters));
        return !clusters.isEmpty() && active == 0;
    }

    private static long countClustersWithState(List<ClusterSummary> clusters, List<String> states) {
        return clusters.stream()
                .filter(cluster -> states.contains(cluster.getStatus().getState()))
                .count();
    }

    private static Map<String, Integer> countClustersByState(List<ClusterSummary> clusters) {
        Map<String, Integer> counts = new HashMap<>();
        for (ClusterSummary cluster : clusters) {
            counts.compute(cluster.getStatus().getState(), WaitForEMRClusters::incrementCount);
        }
        return counts;
    }

    private static Integer incrementCount(String key, Integer countBefore) {
        if (countBefore == null) {
            return 1;
        } else {
            return countBefore + 1;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        String instanceId = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

        WaitForEMRClusters wait = new WaitForEMRClusters(emrClient, systemTestProperties);
        wait.pollUntilFinished();
        s3Client.shutdown();
        emrClient.shutdown();
    }
}
