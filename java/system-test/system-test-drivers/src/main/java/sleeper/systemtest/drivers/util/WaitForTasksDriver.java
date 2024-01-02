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

import sleeper.clients.util.EmrUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;

import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class WaitForTasksDriver {
    private final InstanceProperties properties;
    private final AmazonElasticMapReduce emrClient;

    public WaitForTasksDriver(InstanceProperties properties, AmazonElasticMapReduce emrClient) {
        this.properties = properties;
        this.emrClient = emrClient;
    }

    public static WaitForTasksDriver forEmr(InstanceProperties properties, AmazonElasticMapReduce emrClient) {
        return new WaitForTasksDriver(properties, emrClient);
    }

    public void waitForTasksToStart(PollWithRetries pollUntilTasksStart) throws InterruptedException {
        pollUntilTasksStart.pollUntil("tasks have started", () ->
                EmrUtils.listActiveClusters(emrClient).getClusters().stream()
                        .anyMatch(cluster -> cluster.getName().startsWith("sleeper-" + properties.get(ID) + "-")));
    }
}
