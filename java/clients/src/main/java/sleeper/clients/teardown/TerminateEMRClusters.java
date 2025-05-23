/*
 * Copyright 2022-2025 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ClusterSummary;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;

import sleeper.core.util.PollWithRetries;
import sleeper.core.util.StaticRateLimit;
import sleeper.core.util.ThreadSleep;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.min;
import static sleeper.clients.util.EmrUtils.listActiveClusters;
import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class TerminateEMRClusters {
    private static final Logger LOGGER = LoggerFactory.getLogger(TerminateEMRClusters.class);

    private final PollWithRetries poll = PollWithRetries
            .intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(15));

    private final EmrClient emrClient;
    private final String clusterPrefix;
    private final StaticRateLimit<ListClustersResponse> listActiveClustersLimit;
    private final ThreadSleep threadSleep;

    public TerminateEMRClusters(EmrClient emrClient, String instanceId, StaticRateLimit<ListClustersResponse> listActiveClustersLimit, ThreadSleep threadSleep) {
        this.emrClient = emrClient;
        this.clusterPrefix = "sleeper-" + instanceId + "-";
        this.listActiveClustersLimit = listActiveClustersLimit;
        this.threadSleep = threadSleep;
    }

    public void run() throws InterruptedException {
        List<ClusterSummary> clusters = activeClusters();
        List<String> clusterIds = clusters.stream()
                .filter(cluster -> cluster.name().startsWith(clusterPrefix))
                .map(ClusterSummary::id)
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
            emrClient.terminateJobFlows(request -> request.jobFlowIds(clusterBatch));
            LOGGER.info("Terminated {} clusters out of {}", endIndex, clusters.size());
            // Sustained limit of 0.5 calls per second
            // See https://docs.aws.amazon.com/general/latest/gr/emr.html
            sleepForSustainedRatePerSecond(0.2, threadSleep);
        }
    }

    private void pollUntilTerminated() throws InterruptedException {
        poll.pollUntil("all EMR clusters terminated", this::allClustersTerminated);
    }

    private boolean allClustersTerminated() {
        List<ClusterSummary> clusters = activeClusters();
        long clustersStillRunning = clusters.stream()
                .filter(cluster -> cluster.name().startsWith(clusterPrefix)).count();
        LOGGER.info("{} clusters are still terminating for instance", clustersStillRunning);
        return clustersStillRunning == 0;
    }

    private List<ClusterSummary> activeClusters() {
        return listActiveClusters(emrClient, listActiveClustersLimit).clusters();
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: <instance-id>");
            return;
        }
        String instanceId = args[0];

        try (EmrClient emrClient = EmrClient.create()) {
            TerminateEMRClusters terminateClusters = new TerminateEMRClusters(emrClient, instanceId, StaticRateLimit.none(), Thread::sleep);
            terminateClusters.run();
        }
    }
}
