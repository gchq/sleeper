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

package sleeper.clients.report.ingest.job;

import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ClusterState;
import software.amazon.awssdk.services.emr.model.ClusterSummary;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;
import software.amazon.awssdk.services.emr.model.StepSummary;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.StaticRateLimit;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;

/**
 * Finds the number of steps in each status in the persistent EMR cluster. Queries the AWS EMR API and counts the number
 * of steps with each status.
 */
public class PersistentEmrStepCount {
    private PersistentEmrStepCount() {
    }

    private static final List<ClusterState> ACTIVE_STATES = List.of(ClusterState.STARTING, ClusterState.BOOTSTRAPPING,
            ClusterState.RUNNING, ClusterState.WAITING, ClusterState.TERMINATING);
    private static final StaticRateLimit<ListClustersResponse> LIST_ACTIVE_CLUSTERS_LIMIT = StaticRateLimit.forMaximumRatePerSecond(0.5);

    /**
     * Finds the number of steps in each status in the persistent EMR cluster. Queries the AWS EMR API and counts the
     * number of steps with each status.
     *
     * @param  instanceProperties the instance properties
     * @param  emrClient          the AWS EMR client
     * @return                    a map from status of EMR execution step, to the number of steps in that state in the
     *                            persistent cluster
     */
    public static Map<String, Integer> byStatus(
            InstanceProperties instanceProperties, EmrClient emrClient) {
        return byStatus(instanceProperties, emrClient, LIST_ACTIVE_CLUSTERS_LIMIT);
    }

    /**
     * Finds the number of steps in each status in the persistent EMR cluster. Queries the AWS EMR API and counts the
     * number of steps with each status.
     *
     * @param  instanceProperties      the instance properties
     * @param  emrClient               the AWS EMR client
     * @param  listActiveClustersLimit the rate limit to stay within when making requests to the AWS EMR API
     * @return                         a map from status of EMR execution step, to the number of steps in that state in
     *                                 the persistent cluster
     */
    public static Map<String, Integer> byStatus(
            InstanceProperties instanceProperties, EmrClient emrClient, StaticRateLimit<ListClustersResponse> listActiveClustersLimit) {
        return getPersistentClusterId(instanceProperties, emrClient, listActiveClustersLimit)
                .map(id -> emrClient.listSteps(request -> request.clusterId(id)).steps())
                .map(PersistentEmrStepCount::countStepsByState)
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

    private static ListClustersResponse listActiveClusters(EmrClient emrClient, StaticRateLimit<ListClustersResponse> rateLimit) {
        return rateLimit.requestOrGetLast(() -> emrClient.listClusters(request -> request.clusterStates(ACTIVE_STATES)));
    }

    private static Map<String, Integer> countStepsByState(List<StepSummary> steps) {
        Map<String, Integer> counts = new HashMap<>();
        for (StepSummary step : steps) {
            counts.compute(step.status().stateAsString(), PersistentEmrStepCount::incrementCount);
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
