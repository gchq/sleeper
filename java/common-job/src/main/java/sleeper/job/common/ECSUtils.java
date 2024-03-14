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
package sleeper.job.common;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.Cluster;
import com.amazonaws.services.ecs.model.DescribeClustersRequest;

import java.util.List;

public class ECSUtils {

    private ECSUtils() {
    }

    public static int getNumPendingAndRunningTasks(String clusterName, AmazonECS ecsClient) throws DescribeClusterException {
        DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest().withClusters(clusterName);
        List<Cluster> clusters = ecsClient.describeClusters(describeClustersRequest).getClusters();
        if (null == clusters || clusters.size() != 1) {
            throw new DescribeClusterException("Unable to retrieve details of cluster " + clusterName);
        }
        return clusters.get(0).getPendingTasksCount() + clusters.get(0).getRunningTasksCount();
    }
}
