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

package sleeper.clients.teardown;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.min;
import static sleeper.clients.util.EmrUtils.listActiveClusters;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class TerminateEMRClusters {
    private static final Logger LOGGER = LoggerFactory.getLogger(TerminateEMRClusters.class);
    private static final long POLL_INTERVAL_MILLIS = 30000;
    private static final int MAX_POLLS = 30;

    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    private final AmazonElasticMapReduce emrClient;
    private final String clusterPrefix;

    public TerminateEMRClusters(AmazonElasticMapReduce emrClient, InstanceProperties properties) {
        this.emrClient = emrClient;
        this.clusterPrefix = "sleeper-" + properties.get(ID) + "-";
    }

    public void run() throws InterruptedException {
        List<ClusterSummary> clusters = listActiveClusters(emrClient).getClusters();
        List<String> clusterIds = clusters.stream()
                .filter(cluster -> cluster.getName().startsWith(clusterPrefix))
                .map(ClusterSummary::getId)
                .collect(Collectors.toList());
        if (clusterIds.isEmpty()) {
            LOGGER.info("No running clusters to terminate");
        } else {
            LOGGER.info("Terminating {} running clusters", clusterIds.size());
            terminateClusters(clusterIds);
            LOGGER.info("Waiting for clusters to terminate");
            pollUntilTerminated();
        }
    }

    private void terminateClusters(List<String> clusters) {
        // Can only terminate 10 clusters at a time
        // See https://docs.aws.amazon.com/emr/latest/APIReference/API_TerminateJobFlows.html
        for (int i = 0; i < clusters.size(); i += 10) {
            int endIndex = min(i + 10, clusters.size());
            List<String> clusterBatch = clusters.subList(i, endIndex);
            emrClient.terminateJobFlows(new TerminateJobFlowsRequest().withJobFlowIds(clusterBatch));
            LOGGER.info("Terminated {} clusters out of {}", endIndex, clusters.size());
            // Sustained limit of 0.5 calls per second
            // See https://docs.aws.amazon.com/general/latest/gr/emr.html
            sleepForSustainedRatePerSecond(0.2);
        }
    }

    private void pollUntilTerminated() throws InterruptedException {
        poll.pollUntil("all EMR clusters terminated", this::allClustersTerminated);
    }

    private boolean allClustersTerminated() {
        List<ClusterSummary> clusters = listActiveClusters(emrClient).getClusters();
        long clustersStillRunning = clusters.stream()
                .filter(cluster -> cluster.getName().startsWith(clusterPrefix)).count();
        LOGGER.info("{} clusters are still terminating for instance", clustersStillRunning);
        return clustersStillRunning == 0;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: <instance-id>");
            return;
        }

        String instanceId = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        InstanceProperties properties = new InstanceProperties();
        properties.loadFromS3GivenInstanceId(s3Client, instanceId);

        if (properties.getList(OPTIONAL_STACKS).contains("EmrBulkImportStack")) {
            AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.defaultClient();
            TerminateEMRClusters terminateClusters = new TerminateEMRClusters(emrClient, properties);
            terminateClusters.run();
            emrClient.shutdown();
        }

        s3Client.shutdown();
    }
}
