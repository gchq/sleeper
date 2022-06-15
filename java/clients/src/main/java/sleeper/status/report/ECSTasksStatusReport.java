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

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.ListTasksResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import sleeper.ClientUtils;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;

/**
 * A utility class to report the number of compaction tasks in progress.
 */
public class ECSTasksStatusReport {
    private final InstanceProperties instanceProperties;
    private final AmazonECS ecsClient;

    public ECSTasksStatusReport(InstanceProperties instanceProperties,
                                AmazonECS ecsClient) {
        this.instanceProperties = instanceProperties;
        this.ecsClient = ecsClient;
    }

    public void run() {
        System.out.println( "\nECS Tasks Status Report:\n--------------------------");
        ListTasksRequest listTasksRequest = new ListTasksRequest()
                .withCluster(instanceProperties.get(COMPACTION_CLUSTER));
        ListTasksResult listTasksResult = ecsClient.listTasks(listTasksRequest);
        System.out.println("Number of compaction tasks = " + listTasksResult.getTaskArns().size());
        listTasksRequest = new ListTasksRequest()
                .withCluster(instanceProperties.get(SPLITTING_COMPACTION_CLUSTER));
        listTasksResult = ecsClient.listTasks(listTasksRequest);
        System.out.println("Number of splitting compaction tasks = " + listTasksResult.getTaskArns().size());
    }

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);
        amazonS3.shutdown();

        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        ECSTasksStatusReport statusReport = new ECSTasksStatusReport(instanceProperties, ecsClient);
        statusReport.run();

        ecsClient.shutdown();
    }
}
