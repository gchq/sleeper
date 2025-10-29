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
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;

import sleeper.clients.deploy.PauseSystem;
import sleeper.clients.util.EmrUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.StaticRateLimit;
import sleeper.core.util.ThreadSleep;

import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class ShutdownSystemProcesses {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownSystemProcesses.class);

    private final CloudWatchEventsClient cloudWatch;
    private final EmrClient emrClient;
    private final StaticRateLimit<ListClustersResponse> listActiveClustersLimit;
    private final ThreadSleep threadSleep;

    public ShutdownSystemProcesses(TearDownClients clients) {
        this(clients.getCloudWatch(), clients.getEmr(), EmrUtils.LIST_ACTIVE_CLUSTERS_LIMIT, Thread::sleep);
    }

    public ShutdownSystemProcesses(
            CloudWatchEventsClient cloudWatch,
            EmrClient emrClient,
            StaticRateLimit<ListClustersResponse> listActiveClustersLimit,
            ThreadSleep threadSleep) {
        this.cloudWatch = cloudWatch;
        this.emrClient = emrClient;
        this.listActiveClustersLimit = listActiveClustersLimit;
        this.threadSleep = threadSleep;
    }

    public void shutdown(InstanceProperties instanceProperties, List<String> extraECSClusters) throws InterruptedException {
        LOGGER.info("Shutting down system processes for instance {}", instanceProperties.get(ID));
        LOGGER.info("Pausing the system");
        PauseSystem.pause(cloudWatch, instanceProperties);
        stopEMRClusters(instanceProperties);
    }

    private void stopEMRClusters(InstanceProperties properties) throws InterruptedException {
        new TerminateEMRClusters(emrClient, properties.get(ID), listActiveClustersLimit, threadSleep).run();
    }

}
