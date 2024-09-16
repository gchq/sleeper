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
package sleeper.clients.util;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;

import sleeper.core.util.StaticRateLimit;

import java.util.List;

public class EmrUtils {
    private EmrUtils() {
    }

    private static final List<String> ACTIVE_STATES = List.of(ClusterState.STARTING.name(), ClusterState.BOOTSTRAPPING.name(),
            ClusterState.RUNNING.name(), ClusterState.WAITING.name(), ClusterState.TERMINATING.name());
    public static final StaticRateLimit<ListClustersResult> LIST_ACTIVE_CLUSTERS_LIMIT = StaticRateLimit.forMaximumRatePerSecond(0.5);

    public static ListClustersResult listActiveClusters(AmazonElasticMapReduce emrClient, StaticRateLimit<ListClustersResult> rateLimit) {
        return rateLimit.requestOrGetLast(() -> emrClient.listClusters(
                new ListClustersRequest().withClusterStates(ACTIVE_STATES)));
    }
}
