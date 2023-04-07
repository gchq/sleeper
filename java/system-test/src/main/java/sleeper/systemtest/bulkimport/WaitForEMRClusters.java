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
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.util.PollWithRetries;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class WaitForEMRClusters {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForEMRClusters.class);
    private static final long POLL_INTERVAL_MILLIS = 60000;
    private static final int MAX_POLLS = 30;

    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    private final AmazonElasticMapReduce emrClient;
    private final String clusterPrefix;
    private static final List<String> STARTING_STATES = List.of(ClusterState.STARTING.name(), ClusterState.BOOTSTRAPPING.name());
    private static final List<String> RUNNING_STATES = List.of(ClusterState.RUNNING.name(), ClusterState.WAITING.name());
    private static final List<String> ACTIVE_STATES = List.of(ClusterState.STARTING.name(), ClusterState.BOOTSTRAPPING.name(),
            ClusterState.RUNNING.name(), ClusterState.WAITING.name(), ClusterState.TERMINATING.name());

    public WaitForEMRClusters(AmazonElasticMapReduce emrClient, InstanceProperties properties) {
        this.emrClient = emrClient;
        this.clusterPrefix = "sleeper-" + properties.get(ID) + "-system-test";
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("all EMR clusters finished", this::allClustersFinished);
    }

    public boolean allClustersFinished() {
        List<ClusterSummary> clusters = emrClient.listClusters(new ListClustersRequest()
                        .withClusterStates(ACTIVE_STATES)).getClusters().stream()
                .filter(cluster -> cluster.getName().startsWith(clusterPrefix))
                .collect(Collectors.toList());
        long clustersStarting = clusters.stream()
                .filter(cluster -> STARTING_STATES.contains(cluster.getStatus().getState())).count();
        long clustersRunning = clusters.stream()
                .filter(cluster -> RUNNING_STATES.contains(cluster.getStatus().getState())).count();
        long clustersTerminating = clusters.size() - clustersRunning - clustersStarting;
        LOGGER.info("Waiting for {} clusters ({} starting, {} running, {} terminating)",
                clusters.size(), clustersStarting, clustersRunning, clustersTerminating);
        return clusters.size() == 0;
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
