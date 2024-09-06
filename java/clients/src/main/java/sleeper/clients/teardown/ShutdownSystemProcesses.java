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
package sleeper.clients.teardown;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.ListTasksResult;
import com.amazonaws.services.ecs.model.StopTaskRequest;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.clients.status.update.PauseSystem;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.function.Consumer;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class ShutdownSystemProcesses {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownSystemProcesses.class);

    private final AmazonCloudWatchEvents cloudWatch;
    private final AmazonECS ecs;
    private final AmazonElasticMapReduce emrClient;
    private final EmrServerlessClient emrServerlessClient;

    public ShutdownSystemProcesses(TearDownClients clients) {
        this(clients.getCloudWatch(), clients.getEcs(), clients.getEmr(), clients.getEmrServerless());
    }

    public ShutdownSystemProcesses(
            AmazonCloudWatchEvents cloudWatch, AmazonECS ecs,
            AmazonElasticMapReduce emrClient, EmrServerlessClient emrServerlessClient) {
        this.cloudWatch = cloudWatch;
        this.ecs = ecs;
        this.emrClient = emrClient;
        this.emrServerlessClient = emrServerlessClient;
    }

    public void shutdown(InstanceProperties instanceProperties, List<String> extraECSClusters) throws InterruptedException {
        LOGGER.info("Shutting down system processes for instance {}", instanceProperties.get(ID));
        LOGGER.info("Pausing the system");
        PauseSystem.pause(cloudWatch, instanceProperties);
        stopECSTasks(instanceProperties, extraECSClusters);
        stopEMRClusters(instanceProperties);
        stopEMRServerlessApplication(instanceProperties);
    }

    private void stopECSTasks(InstanceProperties instanceProperties, List<String> extraClusters) {
        stopTasks(ecs, instanceProperties, INGEST_CLUSTER);
        stopTasks(ecs, instanceProperties, COMPACTION_CLUSTER);
        extraClusters.forEach(clusterName -> stopTasks(ecs, clusterName));
    }

    private void stopEMRClusters(InstanceProperties properties) throws InterruptedException {
        new TerminateEMRClusters(emrClient, properties).run();
    }

    private void stopEMRServerlessApplication(InstanceProperties properties) throws InterruptedException {
        new TerminateEMRServerlessApplications(emrServerlessClient, properties).run();
    }

    public static <T extends SleeperProperty> void stopTasks(AmazonECS ecs, SleeperProperties<T> properties, T property) {
        if (!properties.isSet(property)) {
            return;
        }
        stopTasks(ecs, properties.get(property));
    }

    private static void stopTasks(AmazonECS ecs, String clusterName) {
        LOGGER.info("Stopping tasks for ECS cluster {}", clusterName);
        forEachTaskArn(ecs, clusterName, taskArn -> {
            // Rate limit for ECS StopTask is 100 burst, 40 sustained:
            // https://docs.aws.amazon.com/AmazonECS/latest/APIReference/request-throttling.html
            sleepForSustainedRatePerSecond(30);
            ecs.stopTask(new StopTaskRequest().withCluster(clusterName).withTask(taskArn)
                    .withReason("Cleaning up before cdk destroy"));
        });
    }

    private static void forEachTaskArn(AmazonECS ecs, String clusterName, Consumer<String> consumer) {
        String nextToken = null;
        do {
            ListTasksResult result = ecs.listTasks(
                    new ListTasksRequest().withCluster(clusterName).withNextToken(nextToken));

            LOGGER.info("Found {} tasks", result.getTaskArns().size());
            result.getTaskArns().forEach(consumer);
            nextToken = result.getNextToken();
        } while (nextToken != null);
    }
}
