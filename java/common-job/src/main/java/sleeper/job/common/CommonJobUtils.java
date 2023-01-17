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
import com.amazonaws.services.ecs.model.DesiredStatus;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.ListTasksResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.util.EC2MetadataUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class common to Sleeper components that run jobs in a container and
 * need to consume messages from a queue.
 */
public class CommonJobUtils {
    /** Environment variable key for finding where this program is running.*/
    private static final String EXECUTION_ENV = "AWS_EXECUTION_ENV";
    /** Value if running on EC2.*/
    private static final String ECS_EC2_ENV = "AWS_ECS_EC2";

    private CommonJobUtils() {

    }

    /**
     * Return the instance information about this EC2 instance if this is a container
     * running on EC2 inside ECS.
     *
     * @return an optional containing the instance information
     */
    public static Optional<EC2MetadataUtils.InstanceInfo> getEC2Info() {
        if (ECS_EC2_ENV.equalsIgnoreCase(System.getenv(EXECUTION_ENV))) {
            return Optional.ofNullable(EC2MetadataUtils.getInstanceInfo());
        } else {
            return Optional.empty();
        }
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

    public static int getNumRunningTasks(String clusterName, AmazonECS ecsClient) {
        int numRunningTasks = 0;
        ListTasksRequest listTasksRequest = new ListTasksRequest()
                .withCluster(clusterName)
                .withDesiredStatus(DesiredStatus.RUNNING);
        ListTasksResult listTasksResult = ecsClient.listTasks(listTasksRequest);
        numRunningTasks += listTasksResult.getTaskArns().size();
        while (null != listTasksResult.getNextToken()) {
            listTasksRequest = new ListTasksRequest()
                    .withCluster(clusterName)
                    .withDesiredStatus(DesiredStatus.RUNNING)
                    .withNextToken(listTasksResult.getNextToken());
            listTasksResult = ecsClient.listTasks(listTasksRequest);
            numRunningTasks += listTasksResult.getTaskArns().size();
        }
        return numRunningTasks;
    }
}
