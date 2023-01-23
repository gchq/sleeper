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
package sleeper.job.common;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.Cluster;
import com.amazonaws.services.ecs.model.DescribeClustersRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class common to Sleeper components that run jobs in a container and
 * need to consume messages from a queue.
 */
public class CommonJobUtils {

    private CommonJobUtils() {
    }

    public static Map<String, Integer> getNumberOfMessagesInQueue(String sqsJobQueueUrl, AmazonSQS sqsClient) {
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
                .withQueueUrl(sqsJobQueueUrl)
                .withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages,
                        QueueAttributeName.ApproximateNumberOfMessagesNotVisible);
        GetQueueAttributesResult sizeResult = sqsClient.getQueueAttributes(getQueueAttributesRequest);
        // See https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueAttributes.html
        int appoximateNumberOfMessages = Integer.parseInt(sizeResult.getAttributes().get("ApproximateNumberOfMessages"));
        int approximateNumberOfMessagesNotVisible = Integer.parseInt(sizeResult.getAttributes().get("ApproximateNumberOfMessagesNotVisible"));
        Map<String, Integer> results = new HashMap<>();
        results.put(QueueAttributeName.ApproximateNumberOfMessages.toString(), appoximateNumberOfMessages);
        results.put(QueueAttributeName.ApproximateNumberOfMessagesNotVisible.toString(), approximateNumberOfMessagesNotVisible);
        return results;
    }

    public static int getNumPendingAndRunningTasks(String clusterName, AmazonECS ecsClient) throws DescribeClusterException {
        DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest().withClusters(clusterName);
        List<Cluster> clusters = ecsClient.describeClusters(describeClustersRequest).getClusters();
        if (null == clusters || clusters.isEmpty() || clusters.size() > 1) {
            throw new DescribeClusterException("Unable to retrieve details of cluster " + clusterName);
        }
        return clusters.get(0).getPendingTasksCount() + clusters.get(0).getRunningTasksCount();
    }
}
