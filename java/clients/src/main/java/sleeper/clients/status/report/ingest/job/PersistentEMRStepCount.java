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

package sleeper.clients.status.report.ingest.job;

import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ClusterSummary;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;
import software.amazon.awssdk.services.emr.model.StepSummary;

import sleeper.clients.util.EmrUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.StaticRateLimit;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static sleeper.clients.util.EmrUtils.listActiveClusters;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;

public class PersistentEMRStepCount {
    private PersistentEMRStepCount() {
    }

    public static Map<String, Integer> byStatus(
            InstanceProperties instanceProperties, EmrClient emrClient) {
        return byStatus(instanceProperties, emrClient, EmrUtils.LIST_ACTIVE_CLUSTERS_LIMIT);
    }

    public static Map<String, Integer> byStatus(
            InstanceProperties instanceProperties, EmrClient emrClient, StaticRateLimit<ListClustersResponse> listActiveClustersLimit) {
        return getPersistentClusterId(instanceProperties, emrClient, listActiveClustersLimit)
                .map(id -> emrClient.listSteps(request -> request.clusterId(id)).steps())
                .map(PersistentEMRStepCount::countStepsByState)
                .orElse(Collections.emptyMap());
    }

    private static Optional<String> getPersistentClusterId(
            InstanceProperties instanceProperties, EmrClient emrClient, StaticRateLimit<ListClustersResponse> listActiveClustersLimit) {
        String clusterName = instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME);
        if (clusterName == null) {
            return Optional.empty();
        }
        return listActiveClusters(emrClient, listActiveClustersLimit).clusters().stream()
                .filter(cluster -> clusterName.equals(cluster.name()))
                .map(ClusterSummary::id)
                .findAny();
    }

    private static Map<String, Integer> countStepsByState(List<StepSummary> steps) {
        Map<String, Integer> counts = new HashMap<>();
        for (StepSummary step : steps) {
            counts.compute(step.status().stateAsString(), PersistentEMRStepCount::incrementCount);
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
}
