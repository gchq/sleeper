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
package sleeper.common.task;

import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

/**
 * Finds the number of messages on a queue, and starts up one EC2 or Fargate task for each, up to a
 * configurable maximum.
 */
public class RunBulkExportTasks {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <instance-id> <number-of-tasks>");
            return;
        }

        try (S3Client s3Client = S3Client.create();
                EcsClient ecsClient = EcsClient.create()) {
            String instanceId = args[0];
            int numberOfTasks = Integer.parseInt(args[1]);

            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

            RunDataProcessingTasks.createForBulkExport(instanceProperties, ecsClient)
                    .runToMeetTargetTasks(numberOfTasks);
        }
    }

    private RunBulkExportTasks() {
    }
}
