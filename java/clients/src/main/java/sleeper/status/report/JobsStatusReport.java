/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import sleeper.ClientUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.CommonJobUtils;

import java.io.IOException;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;

/**
 * A utility class to report information about the number of jobs on the compaction
 * queues.
 */
public class JobsStatusReport {
    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;

    public JobsStatusReport(InstanceProperties instanceProperties,
                            AmazonSQS sqsClient) {
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
    }

    public void run() {
        System.out.println("\nJobs Status Report:\n--------------------------");
        Map<String, Integer> stats = CommonJobUtils.getNumberOfMessagesInQueue(instanceProperties.get(COMPACTION_JOB_QUEUE_URL), sqsClient);
        System.out.println(stats);
        stats = CommonJobUtils.getNumberOfMessagesInQueue(instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL), sqsClient);
        System.out.println(stats);
    }

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);
        amazonS3.shutdown();

        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        JobsStatusReport statusReport = new JobsStatusReport(instanceProperties, sqsClient);
        statusReport.run();

        sqsClient.shutdown();
    }
}
